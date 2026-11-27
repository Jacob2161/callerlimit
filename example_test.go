package callerlimit_test

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/jacob2161/callerlimit"
)

// uploads lets 8 uploads run at once, handed to callers in turns.
var uploads = callerlimit.NewSlots[string](&callerlimit.NewSlotsArgs{Name: "uploads", Slots: 8})

func handleUpload(w http.ResponseWriter, req *http.Request) {
	release, err := uploads.Acquire(req.Context(), req.Header.Get("X-API-Key-ID"))
	if overload, ok := errors.AsType[*callerlimit.OverloadError](err); ok {
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(overload.RetryAfter.Seconds()))))
		http.Error(w, "too many uploads at once", http.StatusTooManyRequests)
		return
	}
	if err != nil {
		return // the client went away
	}
	defer release()
	// ... store the upload ...
}

func Example() {
	http.HandleFunc("/upload", handleUpload)
	fmt.Println(uploads.Occupancy().Slots)
	// Output: 8
}
