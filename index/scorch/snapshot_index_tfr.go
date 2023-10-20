//  Copyright (c) 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scorch

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

var reflectStaticSizeIndexSnapshotTermFieldReader int

func init() {
	var istfr IndexSnapshotTermFieldReader
	reflectStaticSizeIndexSnapshotTermFieldReader = int(reflect.TypeOf(istfr).Size())
}

// have a new type, a struct where everything to do with TFRs is a slice

// if it's a next() call, just keep going till it's nil, for both arrays.
type IndexSnapshotTermFieldReader struct {
	term               []byte
	field              string
	snapshot           *IndexSnapshot
	dicts              [][]segment.TermDictionary
	postings           [][]segment.PostingsList
	iterators          [][]segment.PostingsIterator
	segmentOffset      []int
	includeFreq        bool
	includeNorm        bool
	includeTermVectors bool
	currPosting        []segment.Posting
	currID             []index.IndexInternalID
	recycle            bool
	bytesRead          uint64
	ctx                context.Context

	sliceOffset []int

	// offset for which segment we are currently reading from
	// overall in Next() and Advance()
	nextSegOffset   int
	nextSliceOffset int
	// Needed for sequential access eg. in Next()
	totalIterator []segment.PostingsIterator
}

func (i *IndexSnapshotTermFieldReader) incrementBytesRead(val uint64) {
	i.bytesRead += val
}

func (i *IndexSnapshotTermFieldReader) Size() int {
	sizeInBytes := reflectStaticSizeIndexSnapshotTermFieldReader + size.SizeOfPtr +
		len(i.term) +
		len(i.field)

	for _, entry := range i.currID {
		sizeInBytes += len(entry)
	}

	for _, slicePosting := range i.postings {
		for _, entry := range slicePosting {
			sizeInBytes += entry.Size()
		}
	}

	for _, sliceItr := range i.iterators {
		for _, entry := range sliceItr {
			sizeInBytes += entry.Size()
		}
	}

	for _, sliceCurrPosting := range i.currPosting {
		if sliceCurrPosting != nil {
			sizeInBytes += sliceCurrPosting.Size()
		}
	}

	return sizeInBytes
}

func (i *IndexSnapshotTermFieldReader) Next(preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	rv := preAlloced
	if rv == nil {
		rv = &index.TermFieldDoc{}
	}
	// find the next hit
	for i.nextSegOffset < len(i.totalIterator) {
		prevBytesRead := i.totalIterator[i.nextSegOffset].BytesRead()
		next, err := i.totalIterator[i.nextSegOffset].Next()
		if err != nil {
			return nil, err
		}
		if next != nil {
			// make segment number into global number by adding offset
			// DOUBLE CHECK THIS!
			globalOffset := i.snapshot.offsets[i.nextSegOffset]
			nnum := next.Number()
			rv.ID = docNumberToBytes(rv.ID, nnum+globalOffset)
			i.postingToTermFieldDoc(next, rv)

			i.currID[0] = rv.ID
			i.currPosting[0] = next
			// postingsIterators is maintain the bytesRead stat in a cumulative fashion.
			// this is because there are chances of having a series of loadChunk calls,
			// and they have to be added together before sending the bytesRead at this point
			// upstream.
			bytesRead := i.totalIterator[i.nextSegOffset].BytesRead()
			if bytesRead > prevBytesRead {
				i.incrementBytesRead(bytesRead - prevBytesRead)
			}
			return rv, nil
		}
		i.nextSegOffset++
		slice := i.snapshot.sliceForASegOffset(i.nextSegOffset)
		if slice != i.nextSliceOffset {
			i.nextSliceOffset = slice
		}
	}
	return nil, nil
}

func (i *IndexSnapshotTermFieldReader) NumSlices() int {
	return len(i.snapshot.slices)
}

func (i *IndexSnapshotTermFieldReader) NextInSlice(sliceIndex int, preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	rv := &index.TermFieldDoc{}
	// find the next hit
	for i.segmentOffset[sliceIndex] < len(i.iterators[sliceIndex]) {
		prevBytesRead := i.iterators[sliceIndex][i.segmentOffset[sliceIndex]].BytesRead()
		next, err := i.iterators[sliceIndex][i.segmentOffset[sliceIndex]].Next()
		if err != nil {
			return nil, err
		}
		if next != nil {
			// make segment number into global number by adding offset
			// globalSegOffset --> global offset of seg number
			globalSegOffset := i.segmentOffset[sliceIndex] + i.sliceOffset[sliceIndex]
			globalOffset := i.snapshot.offsets[globalSegOffset]
			nnum := next.Number()
			rv.ID = docNumberToBytes(rv.ID, nnum+globalOffset)
			i.postingToTermFieldDoc(next, rv)

			i.currID[sliceIndex] = rv.ID
			i.currPosting[sliceIndex] = next
			// postingsIterators is maintain the bytesRead stat in a cumulative fashion.
			// this is because there are chances of having a series of loadChunk calls,
			// and they have to be added together before sending the bytesRead at this point
			// upstream.
			bytesRead := i.iterators[sliceIndex][i.segmentOffset[sliceIndex]].BytesRead()
			if bytesRead > prevBytesRead {
				i.incrementBytesRead(bytesRead - prevBytesRead)
			}
			return rv, nil
		}
		i.nextSegOffset++
		i.segmentOffset[sliceIndex]++
	}
	return nil, nil
}

func (i *IndexSnapshotTermFieldReader) postingToTermFieldDoc(next segment.Posting, rv *index.TermFieldDoc) {
	if i.includeFreq {
		rv.Freq = next.Frequency()
	}
	if i.includeNorm {
		rv.Norm = next.Norm()
	}
	if i.includeTermVectors {
		locs := next.Locations()
		if cap(rv.Vectors) < len(locs) {
			rv.Vectors = make([]*index.TermFieldVector, len(locs))
			backing := make([]index.TermFieldVector, len(locs))
			for i := range backing {
				rv.Vectors[i] = &backing[i]
			}
		}
		rv.Vectors = rv.Vectors[:len(locs)]
		for i, loc := range locs {
			*rv.Vectors[i] = index.TermFieldVector{
				Start:          loc.Start(),
				End:            loc.End(),
				Pos:            loc.Pos(),
				ArrayPositions: loc.ArrayPositions(),
				Field:          loc.Field(),
			}
		}
	}
}

func (i *IndexSnapshotTermFieldReader) Advance(ID index.IndexInternalID, preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	// Find out which slice the segment is in --> then for that slice,
	// update the segment offset.
	// So both Next() and NextInSlice() calls will work seamlessly.
	slice := i.snapshot.sliceForASegOffset(i.nextSegOffset)

	// FIXME do something better
	// for now, if we need to seek backwards, then restart from the beginning
	if i.currPosting != nil && bytes.Compare(i.currID[slice], ID) >= 0 {
		i2, err := i.snapshot.PerSliceTFR(nil, i.term, i.field,
			i.includeFreq, i.includeNorm, i.includeTermVectors)
		if err != nil {
			return nil, err
		}
		// close the current term field reader before replacing it with a new one
		_ = i.Close()
		*i = *(i2.(*IndexSnapshotTermFieldReader))
	}
	num, err := docInternalToNumber(ID)
	if err != nil {
		return nil, fmt.Errorf("error converting to doc number % x - %v", ID, err)
	}
	segIndex, ldocNum := i.snapshot.segmentIndexAndLocalDocNumFromGlobal(num)
	if segIndex >= len(i.snapshot.segment) {
		return nil, fmt.Errorf("computed segment index %d out of bounds %d",
			segIndex, len(i.snapshot.segment))
	}

	// Need to advance the nextSegOffset to the right segment
	// so for subsequent Next() calls, will skip directly to this segment.
	i.nextSegOffset = segIndex

	// Advancing to the segment offset of a specific slice.
	i.segmentOffset[slice] = segIndex

	next, err := i.totalIterator[segIndex].Advance(ldocNum)
	if err != nil {
		return nil, err
	}
	if next == nil {
		// we jumped directly to the segment that should have contained it
		// but it wasn't there, so reuse Next() which should correctly
		// get the next hit after it (we moved i.segmentOffset)
		return i.Next(preAlloced)
	}

	if preAlloced == nil {
		preAlloced = &index.TermFieldDoc{}
	}
	preAlloced.ID = docNumberToBytes(preAlloced.ID, next.Number()+
		i.snapshot.offsets[segIndex])
	i.postingToTermFieldDoc(next, preAlloced)
	i.currID[slice] = preAlloced.ID
	i.currPosting[slice] = next
	return preAlloced, nil
}

func (i *IndexSnapshotTermFieldReader) Count() uint64 {
	var rv uint64
	for _, slicePosting := range i.postings {
		for _, posting := range slicePosting {
			rv += posting.Count()
		}
	}
	return rv
}

func (i *IndexSnapshotTermFieldReader) Close() error {
	if i.ctx != nil {
		statsCallbackFn := i.ctx.Value(search.SearchIOStatsCallbackKey)
		if statsCallbackFn != nil {
			// essentially before you close the TFR, you must report this
			// reader's bytesRead value
			statsCallbackFn.(search.SearchIOStatsCallbackFunc)(i.bytesRead)
		}

		search.RecordSearchCost(i.ctx, search.AddM, i.bytesRead)
	}

	if i.snapshot != nil {
		atomic.AddUint64(&i.snapshot.parent.stats.TotTermSearchersFinished, uint64(1))
		i.snapshot.recycleTermFieldReader(i)
	}
	return nil
}
