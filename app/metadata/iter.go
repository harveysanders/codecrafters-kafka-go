// +go:build go1.23
package metadata

import (
	"fmt"
	"iter"
)

// BatchRecords returns an iterator over the batches in the metadata log file.
// It returns a single-use iterator.
func (l *LogFile) BatchRecords() iter.Seq2[int, RecordBatch] {
	return func(yield func(int, RecordBatch) bool) {
		rb := RecordBatch{}

		n, err := rb.ReadFrom(&l.rdr)
		if err != nil {
			l.err = fmt.Errorf("read record batch: %w", err)
			return
		}

		if !yield(int(rb.Offset), rb) {
			// Cleanup
			return
		}

		// read off the diff from current head and full batch length
		// before moving to the next batch
		toRead := int64(rb.Length) - n
		nDiscarded, err := l.rdr.Discard(int(toRead))
		if err != nil {
			l.err = fmt.Errorf("discard bytes before next batch: %w", err)
			return
		}
		if toRead != int64(nDiscarded) {
			l.err = fmt.Errorf("expected to discard %d bytes, but actually discarded %d", toRead, nDiscarded)
			return
		}
	}
}
