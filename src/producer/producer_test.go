package producer

import (
	"fmt"
	"testing"
)

func Test_handler_divideCurrencies(t *testing.T) {
	curr, err := getCurrencies()
	if err != nil {
		t.Error(err)
	}
	h := handler{goroutines: 3}
	divided := h.divideCurrencies(curr)
	fmt.Println(divided)
}
