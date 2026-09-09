package httpapi

// Keyspace data handlers (§10.1): SCAN, typed key preview, and DEL over
// HTTP. All run through the command engine on a synthetic ConnState (D3);
// value previews are bounded to the first 100 elements per collection.

import (
	"net/http"
	"strconv"

	httpapigen "github.com/pschlump/ultima/gen/httpapi"
	"github.com/pschlump/ultima/lib/resp"
)

// previewLimit bounds collection value previews (GET /api/v1/key/{key}).
const previewLimit = 100

// zsetMember is the ZSetMember wire shape (not generated: KeyPreview.value
// is free-form in the contract, so the type never appears in gen/httpapi).
type zsetMember struct {
	Member string `json:"member"`
	Score  string `json:"score"` // string2d-exact, as on the wire
}

// paramDB resolves the optional ?db= query parameter (default 0).
func paramDB(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// ScanKeys implements GET /api/v1/keys/scan: one SCAN step with the
// optional MATCH/COUNT hints.
func (s *Server) ScanKeys(w http.ResponseWriter, r *http.Request, params httpapigen.ScanKeysParams) {
	cursor := "0"
	if params.Cursor != nil {
		cursor = *params.Cursor
	}
	args := []string{"SCAN", cursor}
	if params.Match != nil {
		args = append(args, "MATCH", *params.Match)
	}
	if params.Count != nil {
		args = append(args, "COUNT", strconv.Itoa(*params.Count))
	}
	v := s.exec(r, paramDB(params.Db), args...)
	if v.Kind == resp.KindError {
		writeError(w, http.StatusBadRequest, v.Str)
		return
	}
	// Reply is the 2-element array [cursor-blob, keys-array].
	out := httpapigen.ScanResult{Keys: []string{}}
	if len(v.Arr) == 2 {
		out.Cursor = valStr(v.Arr[0])
		for _, k := range v.Arr[1].Arr {
			out.Keys = append(out.Keys, valStr(k))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// GetKey implements GET /api/v1/key/{key}: TYPE, PTTL, and a bounded
// type-shaped value preview.
func (s *Server) GetKey(w http.ResponseWriter, r *http.Request, key string, params httpapigen.GetKeyParams) {
	db := paramDB(params.Db)
	typ := valStr(s.exec(r, db, "TYPE", key))
	if typ == "none" {
		writeError(w, http.StatusNotFound, "not_found")
		return
	}
	reply := httpapigen.KeyPreview{Key: key, Type: httpapigen.KeyPreviewType(typ)}
	if v := s.exec(r, db, "PTTL", key); v.Kind == resp.KindInt {
		reply.TtlMs = v.Int
	}
	switch typ {
	case "string":
		reply.Value = valStr(s.exec(r, db, "GET", key))
	case "hash":
		// HSCAN key 0 COUNT 100 → [cursor, flat field/value pairs].
		m := map[string]string{}
		if v := s.exec(r, db, "HSCAN", key, "0", "COUNT", strconv.Itoa(previewLimit)); len(v.Arr) == 2 {
			pairs := v.Arr[1].Arr
			for i := 0; i+1 < len(pairs); i += 2 {
				m[valStr(pairs[i])] = valStr(pairs[i+1])
			}
		}
		reply.Value = m
	case "list":
		reply.Value = s.blobArray(r, db, "LRANGE", key, "0", strconv.Itoa(previewLimit-1))
	case "set":
		// SSCAN key 0 COUNT 100 → [cursor, members].
		members := []string{}
		if v := s.exec(r, db, "SSCAN", key, "0", "COUNT", strconv.Itoa(previewLimit)); len(v.Arr) == 2 {
			for _, m := range v.Arr[1].Arr {
				members = append(members, valStr(m))
			}
		}
		reply.Value = members
	case "zset":
		// ZRANGE key 0 99 WITHSCORES → flat member/score pairs (RESP2).
		members := []zsetMember{}
		v := s.exec(r, db, "ZRANGE", key, "0", strconv.Itoa(previewLimit-1), "WITHSCORES")
		for i := 0; i+1 < len(v.Arr); i += 2 {
			members = append(members, zsetMember{Member: valStr(v.Arr[i]), Score: valStr(v.Arr[i+1])})
		}
		reply.Value = members
	}
	writeJSON(w, http.StatusOK, reply)
}

// blobArray renders an array reply as a []string (never null).
func (s *Server) blobArray(r *http.Request, db int, args ...string) []string {
	out := []string{}
	v := s.exec(r, db, args...)
	for _, e := range v.Arr {
		out = append(out, valStr(e))
	}
	return out
}

// DeleteKey implements DELETE /api/v1/key/{key} (DEL).
func (s *Server) DeleteKey(w http.ResponseWriter, r *http.Request, key string, params httpapigen.DeleteKeyParams) {
	v := s.exec(r, paramDB(params.Db), "DEL", key)
	if v.Kind == resp.KindError {
		writeError(w, http.StatusBadRequest, v.Str)
		return
	}
	writeJSON(w, http.StatusOK, httpapigen.KeyDeleted{Deleted: v.Int > 0})
}
