package searcher

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	index "github.com/blevesearch/bleve_index_api"
)

func setupIndex(t *testing.T) index.Index {
	analysisQueue := index.NewAnalysisQueue(1)
	i, err := scorch.NewScorch(
		gtreap.Name,
		map[string]interface{}{
			"path":          "",
			"spatialPlugin": "s2",
		},
		analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Open()
	if err != nil {
		t.Fatal(err)
	}

	return i
}

func TestPointIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{2.0, 2.0},
			DocShapeVertices: []float64{2.0, 2.0},
			DocShapeName:     "point1",
			Desc:             "coincident points",
			Expected:         []string{"point1"},
		},
		{
			QueryShape:       []float64{2.0, 2.0},
			DocShapeVertices: []float64{2.0, 2.1},
			DocShapeName:     "point2",
			Desc:             "non coincident points",
			Expected:         nil,
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		// indexing and searching independently for each case.
		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("intersects",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPointLinestringIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{4.0, 4.0},
			DocShapeVertices: [][]float64{{2.0, 2.0}, {3.0, 3.0}, {4.0, 4.0}},
			DocShapeName:     "linestring1",
			Desc:             "point at the vertex of linestring",
			Expected:         []string{"linestring1"},
		},
		{
			QueryShape:       []float64{1.5, 1.5001714},
			DocShapeVertices: [][]float64{{0.0, 0.0}, {1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeName:     "linestring1",
			Desc:             "point along linestring",
			Expected:         nil, // nil since point is said to intersect only when it matches any
			// of the endpoints of the linestring
		},
		{
			QueryShape:       []float64{1.5, 1.6001714},
			DocShapeVertices: [][]float64{{0.0, 0.0}, {1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeName:     "linestring1",
			Desc:             "point outside linestring",
			Expected:         nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("intersects",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPointPolygonIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{3.0, 3.0},
			DocShapeVertices: [][][]float64{{{2.0, 2.0}, {3.0, 3.0}, {1.0, 3.0}, {2.0, 2.0}}},
			DocShapeName:     "polygon1",
			Desc:             "point on polygon vertex",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape:       []float64{1.5, 1.500714},
			DocShapeVertices: [][][]float64{{{1.0, 1.0}, {2.0, 2.0}, {0.0, 2.0}, {1.0, 1.0}}},
			DocShapeName:     "polygon1",
			Desc:             "point on polygon edge",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape:       []float64{1.5, 1.9},
			DocShapeVertices: [][][]float64{{{1.0, 1.0}, {2.0, 2.0}, {0.0, 2.0}, {1.0, 1.0}}},
			DocShapeName:     "polygon1",
			Desc:             "point inside polygon",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape: []float64{0.3, 0.3},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeName: "polygon1",
			Desc:         "point inside hole inside polygon",
			Expected:     nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("intersects",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnvelopePointIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: rightRectPoint,
			DocShapeName:     "point1",
			Desc:             "point on vertex of bounded rectangle",
			Expected:         []string{"point1"},
			QueryType:        "intersects",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeEnvelopeRelationQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for Envelope: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
	}
}

func TestMultiPointIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][]float64{{3.0, 3.0}, {4.0, 4.0}},
			DocShapeVertices: [][]float64{{4.0, 4.0}},
			DocShapeName:     "multipoint1",
			Desc:             "single coincident multipoint",
			Expected:         []string{"multipoint1"},
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "multipoint", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("intersects",
				true, indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipoint: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinestringIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][]float64{{3.0, 2.0}, {4.0, 2.0}},
			DocShapeVertices: [][]float64{{3.0, 2.0}, {4.0, 2.0}},
			DocShapeName:     "linestring1",
			Desc:             "coincident linestrings",
			Expected:         []string{"linestring1"},
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {1.5, 1.5}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{2.0, 2.0}, {4.0, 3.0}},
			DocShapeName:     "linestring1",
			Desc:             "linestrings intersecting at the ends",
			Expected:         []string{"linestring1"},
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {3.0, 3.0}},
			DocShapeVertices: [][]float64{{1.5499860, 1.5501575}, {4.0, 6.0}},
			DocShapeName:     "linestring1",
			Desc:             "subline not at vertex",
			Expected:         []string{"linestring1"},
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{1.5499860, 1.5501575}, {1.5, 1.5001714}},
			DocShapeName:     "linestring1",
			Desc:             "subline inside linestring",
			Expected:         []string{"linestring1"},
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {1.5, 1.5}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{1.0, 2.0}, {2.0, 1.0}},
			DocShapeName:     "linestring1",
			Desc:             "linestrings intersecting at some edge",
			Expected:         []string{"linestring1"},
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {1.5, 1.5}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{1.0, 2.0}, {1.0, 4.0}},
			DocShapeName:     "linestring1",
			Desc:             "non intersecting linestrings",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{59.32, 0.52}, {68.99, -7.36}, {75.49, -12.21}},
			DocShapeVertices: [][]float64{{71.98, 0}, {67.58, -6.57}, {63.19, -12.72}},
			DocShapeName:     "linestring1",
			Desc:             "linestrings with more than 2 points intersecting at some edges",
			Expected:         []string{"linestring1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinestringPolygonIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {1.5, 1.5}, {2.0, 2.0}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring intersects polygon at a vertex",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape:       [][]float64{{0.2, 0.2}, {0.4, 0.4}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring within polygon",
			Expected:         nil, // nil since linestring intersects polygon only if it intersects any of the polygons edges
		},
		{
			QueryShape:       [][]float64{{-0.5, 0.5}, {0.5, 0.5}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring intersects polygon at an edge",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape:       [][]float64{{-0.5, 0.5}, {1.5, 0.5}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring intersects polygon as a whole",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape:       [][]float64{{-0.5, 0.5}, {-1.5, -1.5}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring does not intersect polygon",
			Expected:         nil,
		},
		{
			QueryShape: [][]float64{{0.3, 0.3}, {0.35, 0.35}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeName: "polygon1",
			Desc:         "linestring does not intersect polygon when contained in the hole",
			Expected:     nil,
		},
		{
			QueryShape: [][]float64{{0.3, 0.3}, {0.5, 0.5}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeName: "polygon1",
			Desc:         "linestring intersects polygon in the hole",
			Expected:     []string{"polygon1"},
		},
		{
			QueryShape: [][]float64{{0.4, 0.3}, {0.6, 0.3}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.3, 0.3}, {0.4, 0.2}, {0.5, 0.3}, {0.4, 0.4}, {0.3, 0.3}},
				{{0.5, 0.3}, {0.6, 0.2}, {0.7, 0.3}, {0.6, 0.4}, {0.5, 0.3}}},
			DocShapeName: "polygon1",
			Desc:         "linestring intersects polygon through touching holes",
			Expected:     []string{"polygon1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinestringPointIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][]float64{{179, 0}, {-179, 0}},
			DocShapeVertices: []float64{179.1, 0},
			DocShapeName:     "point1",
			Desc:             "point across longitudinal boundary of linestring",
			Expected:         []string{"point1"},
		},
		{
			QueryShape:       [][]float64{{-179, 0}, {179, 0}},
			DocShapeVertices: []float64{179.1, 0},
			DocShapeName:     "point1",
			Desc:             "point across longitudinal boundary of reversed linestring",
			Expected:         []string{"point1"},
		},
		{
			QueryShape:       [][]float64{{179, 0}, {-179, 0}},
			DocShapeVertices: []float64{170, 0},
			DocShapeName:     "point1",
			Desc:             "point does not intersect linestring",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{-179, 0}, {179, 0}},
			DocShapeVertices: []float64{170, 0},
			DocShapeName:     "point1",
			Desc:             "point does not intersect reversed linestring",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{-179, 0}, {179, 0}, {178, 0}},
			DocShapeVertices: []float64{178, 0},
			DocShapeName:     "point1",
			Desc:             "point intersects linestring with multiple points",
			Expected:         []string{"point1"},
		},
		{
			QueryShape:       [][]float64{{-179, 0}, {179, 0}, {178, 0}, {180, 0}},
			DocShapeVertices: []float64{178, 0},
			DocShapeName:     "point1",
			Desc:             "point intersects linestring with multiple points",
			Expected:         []string{"point1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiLinestringIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][][]float64{{{1.0, 1.0}, {1.1, 1.1}, {2.0, 2.0}, {2.1, 2.1}}},
			DocShapeVertices: [][][]float64{{{0.0, 0.5132}, {-1.1, -1.1}, {1.5, 1.512}, {2.1, 2.1}}},
			DocShapeName:     "multilinestring1",
			Desc:             "intersecting multilinestrings",
			Expected:         []string{"multilinestring1"},
		},
		{
			QueryShape:       [][][]float64{{{1.0, 1.0}, {1.1, 1.1}, {2.0, 2.0}, {2.1, 2.1}}},
			DocShapeVertices: [][][]float64{{{100.1, 100.5}, {101.5, 102.5}}},
			DocShapeName:     "multilinestring1",
			Desc:             "non-intersecting multilinestrings",
			Expected:         nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "multilinestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiLinestringIntersectsQuery("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multilinestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiLinestringMultiPointIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][][]float64{{{2.0, 2.0}, {2.1, 2.1}}, {{3.0, 3.0}, {3.1, 3.1}}},
			DocShapeVertices: [][]float64{{5.0, 6.0}, {67, 67}, {3.1, 3.1}},
			DocShapeName:     "multipoint1",
			Desc:             "multilinestring intersects one of the multipoints",
			Expected:         []string{"multipoint1"},
		},
		{
			QueryShape:       [][][]float64{{{2.0, 2.0}, {2.1, 2.1}}, {{3.0, 3.0}, {3.1, 3.1}}},
			DocShapeVertices: [][]float64{{56.0, 56.0}, {66, 66}},
			DocShapeName:     "multipoint1",
			Desc:             "multilinestring intersects one of the multipoints",
			Expected:         nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "multipoint", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiLinestringIntersectsQuery("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPolygonIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeName: "polygon1",
			Desc:         "coincident polygons",
			Expected:     []string{"polygon1"},
		},
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{1.2, 1.2}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.2, 1.2}}},
			DocShapeName: "polygon1",
			Desc:         "polygon and a window polygon",
			Expected:     []string{"polygon1"},
		},
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{1.1, 1.1}, {1.2, 1.1}, {1.2, 1.2},
				{1.1, 1.2}, {1.1, 1.1}}},
			DocShapeName: "polygon1",
			Desc:         "nested polygons",
			Expected:     []string{"polygon1"},
		},
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{0.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{0.0, 2.0}, {0.0, 1.0}}},
			DocShapeName: "polygon1",
			Desc:         "intersecting polygons",
			Expected:     []string{"polygon1"},
		},
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{3.0, 3.0}, {4.0, 3.0}, {4.0, 4.0},
				{3.0, 4.0}, {3.0, 3.0}}},
			DocShapeName: "polygon1",
			Desc:         "disjoint polygons",
			Expected:     nil,
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPolygonPointIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][][]float64{{{150, 85}, {160, 85}, {-20, 85}, {-30, 85}, {150, 85}}},
			DocShapeVertices: []float64{150, 88},
			DocShapeName:     "point1",
			Desc:             "polygon intersects point in latitudinal boundary",
			Expected:         []string{"point1"},
		},
		{
			QueryShape:       [][][]float64{{{150, 85}, {160, 85}, {-20, 85}, {-30, 85}, {150, 85}}},
			DocShapeVertices: []float64{170, 88},
			DocShapeName:     "point1",
			Desc:             "polygon does not intersects point outside latitudinal boundary",
			Expected:         nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiPolygonIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20},
				{5, 10}, {15, 5}}}, {{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][][]float64{{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0},
				{0.0, 1.0}, {0.0, 0.0}}, {{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeName: "multipolygon1",
			Desc:         "intersecting multi polygons",
			Expected:     []string{"multipolygon1"},
		},
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20},
				{5, 10}, {15, 5}}}, {{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][][]float64{{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0},
				{0.0, 1.0}, {0.0, 0.0}}}},
			DocShapeName: "multipolygon1",
			Desc:         "non intersecting multi polygons",
			Expected:     nil,
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			test.DocShapeVertices, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiPolygonMultiPointIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape: [][][][]float64{{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}},
				{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}}},
			DocShapeVertices: [][]float64{{30, 20}, {30, 30}},
			DocShapeName:     "multipoint1",
			Desc:             "multipolygon intersects multipoint",
			Expected:         []string{"multipoint1"},
		},
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
				{{{30, 20}, {45, 50}, {10, 50}, {30, 20}}}},
			DocShapeVertices: [][]float64{{30, -20}, {-30, 30}, {45, 66}},
			DocShapeName:     "multipoint1",
			Desc:             "multipolygon does not intersect multipoint",
			Expected:         nil,
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "multipoint", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiPolygonMultiLinestringIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}}, {{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][]float64{{{65, 40}, {60, 40}}, {{45, 40}, {10, 40}, {30, 20}}},
			DocShapeName:     "multilinestring1",
			Desc:             "multipolygon intersects multilinestring",
			Expected:         []string{"multilinestring1"},
		},
		{
			QueryShape:       [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}}, {{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][]float64{{{45, 41}, {60, 80}}, {{-45, -40}, {-10, -40}}},
			DocShapeName:     "multilinestring1",
			Desc:             "multipolygon does not intersect multilinestring",
			Expected:         nil,
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "multilinestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation("intersects",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGeometryCollectionIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][][]float64
		DocShapeVertices [][][][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       nil,
			DocShapeVertices: nil,
			DocShapeName:     "geometrycollection1",
			Desc:             "empty geometry collections",
			Expected:         nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
			[]uint64{}, test.DocShapeVertices, nil, document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeGeometryCollectionRelationQuery("intersects",
				indexReader, test.QueryShape, nil, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for geometry collection: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGeometryCollectionPointIntersects(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
		Types            []string
	}{
		{
			QueryShape:       []float64{1.0, 2.0},
			DocShapeVertices: nil,
			DocShapeName:     "geometrycollection1",
			Desc:             "geometry collection does not intersect with a point",
			Expected:         nil,
			Types:            nil,
		},
		{
			QueryShape:       []float64{1.0, 2.0},
			DocShapeVertices: [][][][][]float64{{{{{1.0, 2.0}}}}},
			DocShapeName:     "geometrycollection1",
			Desc:             "geometry collection intersects with a point",
			Expected:         []string{"geometrycollection1"},
			Types:            []string{"point"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
			[]uint64{}, test.DocShapeVertices, test.Types, document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("intersects",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
