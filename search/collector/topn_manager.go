package collector

import (
	"strconv"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

type TopNCollectorManager struct {
	size      int
	skip      int
	total     uint64
	bytesRead uint64
	maxScore  float64
	// Sum of all 'took' fields of the child collectors
	took time.Duration
	// TODO Using this strategy too much - having a lock for the mgr
	// and using it whenever a child coll needs to update a mgr field
	// any alternative?

	// Alternative is a map-reduce like strategy where each child coll's
	// results are merged into the mgr's results at the end.
	// That will add a separate reduce phase at the end though
	tookMutext sync.Mutex

	sort          search.SortOrder
	results       search.DocumentMatchCollection
	facetsBuilder *search.FacetsBuilder

	// merge results from these collections into the store
	store collectorStore
	// mutex to serialise access to mgr from child colls.
	childMutex *sync.Mutex

	needDocIds    bool
	neededFields  []string
	cachedScoring []bool
	cachedDesc    []bool

	lowestMatchOutsideResults *search.DocumentMatch
	updateFieldVisitor        index.DocValueVisitor
	dvReader                  index.DocValueReader
	searchAfter               *search.DocumentMatch

	// Need index reader here so that it can be used to fetch the external id
	indexReader index.IndexReader
}

// NewTopNCollector builds a collector to find the top 'size' hits
// skipping over the first 'skip' hits
// ordering hits by the provided sort order
func NewTopNCollectorManager(indexReader index.IndexReader, size int, skip int,
	sort search.SortOrder) *TopNCollectorManager {
	return newTopNCollectorManager(indexReader, size, skip, sort)
}

// NewTopNCollectorAfter builds a collector to find the top 'size' hits
// skipping over the first 'skip' hits
// ordering hits by the provided sort order
func NewTopNCollectorAfterManager(indexReader index.IndexReader, size int, sort search.SortOrder,
	after []string) *TopNCollectorManager {
	rv := newTopNCollectorManager(indexReader, size, 0, sort)
	rv.searchAfter = &search.DocumentMatch{
		Sort: after,
	}

	for pos, ss := range sort {
		if ss.RequiresDocID() {
			rv.searchAfter.ID = after[pos]
		}
		if ss.RequiresScoring() {
			if score, err := strconv.ParseFloat(after[pos], 64); err == nil {
				rv.searchAfter.Score = score
			}
		}
	}

	return rv
}

func newTopNCollectorManager(indexReader index.IndexReader, size int, skip int,
	sort search.SortOrder) *TopNCollectorManager {
	hc := &TopNCollectorManager{childMutex: &sync.Mutex{},
		size: size, skip: skip, sort: sort}

	// pre-allocate space on the store to avoid reslicing
	// unless the size + skip is too large, then cap it
	// everything should still work, just reslices as necessary
	backingSize := size + skip + 1
	if size+skip > PreAllocSizeSkipCap {
		backingSize = PreAllocSizeSkipCap + 1
	}

	if size+skip > 10 {
		hc.store = newStoreHeap(backingSize, func(i, j *search.DocumentMatch) int {
			return hc.sort.Compare(hc.cachedScoring, hc.cachedDesc, i, j)
		})
	} else {
		hc.store = newStoreSlice(backingSize, func(i, j *search.DocumentMatch) int {
			return hc.sort.Compare(hc.cachedScoring, hc.cachedDesc, i, j)
		})
	}

	// these lookups traverse an interface, so do once up-front
	if sort.RequiresDocID() {
		hc.needDocIds = true
	}
	hc.neededFields = sort.RequiredFields()
	hc.cachedScoring = sort.CacheIsScore()
	hc.cachedDesc = sort.CacheDescending()

	hc.indexReader = indexReader

	return hc
}

func (m *TopNCollectorManager) Total() uint64 {
	return m.total
}

func (m *TopNCollectorManager) Took() time.Duration {
	return m.took
}

func (m *TopNCollectorManager) MaxScore() float64 {
	return m.maxScore
}

func (m *TopNCollectorManager) Results() search.DocumentMatchCollection {
	return m.results
}

// finalizeResults starts with the heap containing the final top size+skip
// it now throws away the results to be skipped
// and does final doc id lookup (if necessary)
func (hc *TopNCollectorManager) FinalizeResults() error {
	var err error

	hc.results, err = hc.store.Final(hc.skip, func(doc *search.DocumentMatch) error {
		if doc.ID == "" {
			// look up the id since we need it for lookup
			var err error
			doc.ID, err = hc.indexReader.ExternalID(doc.IndexInternalID)
			if err != nil {
				return err
			}
		}

		doc.Complete(nil)

		return nil
	})

	return err
}

// SetFacetsBuilder registers a facet builder for this collector
func (hc *TopNCollectorManager) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	hc.facetsBuilder = facetsBuilder
	fieldsRequiredForFaceting := facetsBuilder.RequiredFields()
	// for each of these fields, append only if not already there in hc.neededFields.
	for _, field := range fieldsRequiredForFaceting {
		found := false
		for _, neededField := range hc.neededFields {
			if field == neededField {
				found = true
				break
			}
		}
		if !found {
			hc.neededFields = append(hc.neededFields, field)
		}
	}
}

// FacetResults returns the computed facets results
func (hc *TopNCollectorManager) FacetResults() search.FacetResults {
	if hc.facetsBuilder != nil {
		return hc.facetsBuilder.Results()
	}
	return nil
}
