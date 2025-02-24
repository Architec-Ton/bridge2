package bridge

import (
	"encoding/json"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp"
	"tonconnect-bridge/internal/bridge/metrics"
)

var heartbeat = []byte("event: heartbeat\r\n\r\n")

func (s *SSE) handleSubscribe(ctx *fasthttp.RequestCtx, ip string, authorized bool) {
	idsStr := string(ctx.QueryArgs().Peek("client_id"))
	if len(idsStr) == 0 {
		respError(ctx, "client_id is not passed", 400)
		return
	}

	var onFinish func()
	// limit active subscriptions per ip
	if !authorized {
		s.ipMx.Lock()
		u := s.ips[ip]
		if u == nil {
			u = &IP{}
			s.ips[ip] = u

		}

		if u.ActiveConnections+1 > s.MaxConnectionsPerIP {
			s.ipMx.Unlock()
			respError(ctx, "too many subscriptions from your network", 429)
			return
		}
		u.ActiveConnections++
		s.ipMx.Unlock()

		onFinish = func() {
			s.ipMx.Lock()
			if u.ActiveConnections <= 1 {
				delete(s.ips, ip)
			} else {
				u.ActiveConnections--
			}
			s.ipMx.Unlock()
		}
	}

	// var err error
	// Извлечение last_event_id из параметров запроса
	var lastEventId = uint64(time.Now().UnixNano())
	if eventStr := ctx.QueryArgs().Peek("last_event_id"); eventStr != nil {
		var err error
		lastEventId, err = strconv.ParseUint(string(eventStr), 10, 64)
		if err != nil {
			respError(ctx, "last_event_id is invalid", 400)
			return
		}
	}

	ids := strings.SplitN(idsStr, ",", s.MaxClientsPerSubscribe)
	if len(ids) == s.MaxClientsPerSubscribe && strings.IndexByte(ids[s.MaxClientsPerSubscribe-1], ',') != -1 {
		respError(ctx, "too many client_id passed", 400)
		return
	}

	var clients []*Client
	var eventCases []reflect.SelectCase

	for _, id := range ids {
		if id == "" || len(id) > 64 {
			respError(ctx, "invalid client_id", 400)
			return
		}

		cli := s.client(id, true)

		clients = append(clients, cli)
		eventCases = append(eventCases, reflect.SelectCase{
			Chan: reflect.ValueOf(cli.Signal),
			Dir:  reflect.SelectRecv,
		})
	}

	// hijack connection for resource efficient manual control
	ctx.HijackSetNoResponse(true)
	ctx.Hijack(func(conn net.Conn) {
		metrics.Global.ActiveSubscriptions.Inc()
		log.Debug().Strs("clients", ids).Msg("subscribed")
		defer func() {
			for _, cli := range clients {
				atomic.AddInt32(&cli.Subscriptions, -1)
			}
			if onFinish != nil {
				onFinish()
			}
			metrics.Global.ActiveSubscriptions.Dec()
			log.Debug().Strs("clients", ids).Msg("unsubscribed")
		}()

		header := "HTTP/1.1 200 OK\r\n" +
			"Content-Type: text/event-stream\r\n" +
			"Cache-Control: no-cache\r\n" +
			"Connection: keep-alive\r\n"

		if s.EnableCORS {
			header += "Access-Control-Allow-Origin: *\r\n" +
				"Access-Control-Allow-Credentials: true\r\n"
		}
		header += "Transfer-Encoding: chunked\r\n\r\n"
		if _, err := conn.Write([]byte(header)); err != nil {
			return
		}

		pingShard := atomic.AddUint64(&s.connectionsIter, 1) % uint64(s.HeartbeatGroups)
		var lastMessageAt int64
		for {
			ping := *(*chan struct{})(atomic.LoadPointer(&s.pingWaiters[pingShard]))
			idx, _, _ := reflect.Select(append(eventCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ping),
			}))

			now := time.Now().Unix()
			if idx == len(eventCases) { // сигнал пинга
				if now-lastMessageAt < 5 {
					continue
				}
				if err := sendEvent(conn, heartbeat); err != nil {
					return
				}
				lastMessageAt = now
				continue
			}

			log.Debug().Str("client", ids[idx]).Msg("event signal")

			delivered := 0
			maxEventID := lastEventId
			// Выполнение всех накопившихся событий с id > lastEventId
			err := clients[idx].ExecuteAll(lastEventId, func(e *Event) error {
				delivered++
				if e.ID > maxEventID {
					maxEventID = e.ID
				}
				data, _ := json.Marshal(e)
				return sendEvent(conn, []byte("event: message"+
					"\r\nid: "+strconv.FormatUint(e.ID, 10)+
					"\r\ndata: "+string(data)+"\r\n\r\n"))
			})
			if err != nil {
				return // прекращаем обработку при ошибке
			}
			// Обновляем lastEventId, чтобы при следующей выборке доставлять только новые события
			lastEventId = maxEventID

			metrics.Global.DeliveredMessages.Add(float64(delivered))
			lastMessageAt = now
		}
	})
}

func sendEvent(w net.Conn, data []byte) error {
	// Формирование чанка по правилам chunked transfer encoding
	data = append([]byte(strconv.FormatInt(int64(len(data)), 16)+"\r\n"), data...)
	data = append(data, '\r', '\n')

	if _, err := w.Write(data); err != nil {
		return err
	}
	// Если соединение поддерживает flush, сбрасываем буфер
	if flusher, ok := w.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}