package main

import (
	"context"
	"errors"
	"example.com/vector-storage/internal/vector"
	"net/http"
)

func registerSubscriptionHandler(mux *http.ServeMux, backend any) {
	mux.HandleFunc("/v1/changes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		reader, ok := backend.(interface {
			ReadChanges(context.Context, vector.ChangeRequest) (vector.ChangePage, error)
		})
		if !ok {
			reply(w, 501, map[string]string{"error": "change subscription unavailable"})
			return
		}
		var q vector.ChangeRequest
		if e := decode(w, r, &q); e != nil {
			reply(w, 400, map[string]string{"error": e.Error()})
			return
		}
		page, e := reader.ReadChanges(r.Context(), q)
		if e != nil {
			code := 503
			if errors.Is(e, vector.ErrInvalidSubscription) {
				code = 400
			}
			if errors.Is(e, vector.ErrSubscriptionUnavailable) {
				code = 501
			}
			reply(w, code, map[string]string{"error": e.Error()})
			return
		}
		reply(w, 200, page)
	})
}

func registerConsumerHandler(mux *http.ServeMux, backend any) {
	mux.HandleFunc("/v1/consumers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		manager, ok := backend.(interface {
			ManageConsumer(context.Context, vector.ConsumerRequest) (vector.ConsumerState, error)
		})
		if !ok {
			reply(w, 501, map[string]string{"error": "consumer leases unavailable"})
			return
		}
		var q vector.ConsumerRequest
		if e := decode(w, r, &q); e != nil {
			reply(w, 400, map[string]string{"error": e.Error()})
			return
		}
		result, e := manager.ManageConsumer(r.Context(), q)
		if e != nil {
			code := 503
			if errors.Is(e, vector.ErrInvalidSubscription) {
				code = 400
			}
			if errors.Is(e, vector.ErrFenced) || errors.Is(e, vector.ErrConflict) {
				code = 409
			}
			if errors.Is(e, vector.ErrLogCompacted) {
				code = 410
			}
			if errors.Is(e, vector.ErrSubscriptionUnavailable) {
				code = 501
			}
			reply(w, code, map[string]string{"error": e.Error()})
			return
		}
		reply(w, 200, result)
	})
}
