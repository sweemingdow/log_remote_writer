package tcpwriter

import (
	"log"
	"testing"
)

func TestCalcBackoff(t *testing.T) {
	for i := 0; i < 10; i++ {
		log.Printf("attempt:%d, backoff:%d\n", i, 1000*(1<<i))
	}
}
