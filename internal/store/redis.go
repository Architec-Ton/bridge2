package store

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"tonconnect-bridge/internal/bridge"
)

type RedisStore struct {
	client *redis.Client
	key    string // ключ зависит от clientID
}

func NewRedisStore(clientID, addr, password string, db int) *RedisStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisStore{
		client: rdb,
		key:    "events:" + clientID,
	}
}

func (r *RedisStore) Push(event *bridge.Event) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := json.Marshal(event)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal event")
		return false
	}

	score := float64(event.ID)
	// Добавляем событие в отсортированное множество
	if err := r.client.ZAdd(ctx, r.key, redis.Z{
		Score:  score,
		Member: data,
	}).Err(); err != nil {
		log.Error().Err(err).Msg("failed to push event to redis sorted set")
		return false
	}

	// Опционально: удаляем устаревшие события (с score меньше текущего времени)
	currentTime := time.Now().Unix()
	r.client.ZRemRangeByScore(ctx, r.key, "0", strconv.FormatInt(currentTime, 10))
	return true
}

func (r *RedisStore) ExecuteAll(lastEventId uint64, exec func(event *bridge.Event) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Получаем события с ID больше lastEventId
	minScore := strconv.FormatUint(lastEventId+1, 10)
	results, err := r.client.ZRangeByScore(ctx, r.key, &redis.ZRangeBy{
		Min: minScore,
		Max: "+inf",
	}).Result()
	if err != nil {
		return err
	}

	for _, data := range results {
		var event bridge.Event
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return err
		}

		if err := exec(&event); err != nil {
			return err
		}
		log.Debug().Uint64("id", event.ID).Msg("executed event")
	}

	return nil
}
