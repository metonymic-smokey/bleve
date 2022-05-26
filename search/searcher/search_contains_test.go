package searcher

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
)

func TestPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: []float64{1.0, 1.0},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point contains itself",
			QueryType:        "within",
		},
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: []float64{1.0, 1.1},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point does not contain a different point",
			QueryType:        "within",
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

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
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

func TestMultiPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{1.0, 1.0}},
			DocShapeName:     "multipoint1",
			Expected:         []string{"multipoint1"},
			Desc:             "single multipoint common",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}},
			DocShapeVertices: [][]float64{{1.0, 1.0}, {2.0, 2.0}},
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multipoint not covered by multiple multipoints",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "multipoint", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
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

func TestPointLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "point does not cover different linestring",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
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

func TestPointPolygonWithin(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "point not within polygon",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
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

func TestLinestringPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		// expect nil in each case since when linestring is the query shape, it's always nil
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{1.0, 1.0},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point at start of linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{2.0, 2.0},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point in the middle of linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{3.0, 3.0},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point at end of linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{1.5, 1.50017},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point in between linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{4, 5},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point not contained by linestring",
			QueryType:        "within",
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

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
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

func TestMultiPointMultiLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{ // check this one?
			QueryShape:       [][]float64{{2, 2}, {2.1, 2.1}},
			DocShapeVertices: [][][]float64{{{1, 1}, {1.1, 1.1}}, {{2, 2}, {2.1, 2.1}}},
			DocShapeName:     "multilinestring1",
			Expected:         []string{"multilinestring1"},
			Desc:             "multilinestring covering multipoint",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{2, 2}, {1, 1}, {3, 3}},
			DocShapeVertices: [][][]float64{{{1, 1}, {1.1, 1.1}}, {{2, 2}, {2.1, 2.1}}},
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multilinestring not covering multipoint",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "multilinestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				true, indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
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

func TestLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{1, 1}, {2, 2}, {3, 3}},
			DocShapeVertices: [][]float64{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "longer linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1, 1}, {2, 2}, {3, 3}},
			DocShapeVertices: [][]float64{{1, 1}, {2, 2}, {3, 3}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "coincident linestrings",
			QueryType:        "within",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
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

func TestPolygonPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: []float64{0.5, 0.5},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point within polygon",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: []float64{5.5, 5.5},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point not within polygon",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: []float64{5.5, 5.5},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point not within polygon",
			QueryType:        "within",
		},
		{
			QueryShape: [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.4}, {0.2, 0.2}}},
			DocShapeVertices: []float64{0.3, 0.3},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point within polygon hole",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: []float64{1.0, 0.0},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point on polygon vertex",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{1, 1}, {2, 2}, {0, 2}, {1, 1}}},
			DocShapeVertices: []float64{1.5, 1.5001714},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point on polygon vertex edge",
			QueryType:        "within",
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

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
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

func TestPolygonLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][]float64{{0.1, 0.1}, {0.4, 0.4}},
			DocShapeName:     "linestring1",
			Expected:         []string{"linestring1"},
			Desc:             "linestring within polygon",
			QueryType:        "within",
		},
		{
			QueryShape: [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeVertices: [][]float64{{0.3, 0.3}, {0.55, 0.55}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "linestring intersecting with polygon hole",
			QueryType:        "within",
		},
		{
			QueryShape: [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeVertices: [][]float64{{0.3, 0.3}, {4.0, 4.0}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "linestring intersecting with polygon hole and outside",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][]float64{{-1, -1}, {-2, -2}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "linestring outside polygon",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][]float64{{-0.5, -0.5}, {0.5, 0.5}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "linestring intersecting polygon",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
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

func TestPolygonWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "coincident polygon",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{0.2, 0.2}, {1, 0}, {1, 1}, {0, 1}, {0.2, 0.2}}},
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "polygon covers an intersecting window of itself",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{0.1, 0.1}, {0.2, 0.1}, {0.2, 0.2}, {0.1, 0.2}, {0.1, 0.1}}},
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "polygon covers a nested version of itself",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{-1, 0}, {1, 0}, {1, 1}, {-1, 1}, {-1, 0}}},
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "intersecting polygons",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{3, 3}, {4, 3}, {4, 4}, {3, 4}, {3, 3}}},
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "polygon totally out of range",
			QueryType:        "within",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
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

func TestMultiPolygonMultiPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{ // check this one
			QueryShape: [][][][]float64{{{{30, 25}, {45, 40}, {10, 40}, {30, 20}}},
				{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}}},
			DocShapeVertices: [][]float64{{30, 20}, {15, 5}},
			DocShapeName:     "multipoint1",
			Expected:         []string{"multipoint1"},
			Desc:             "multipolygon covers multipoint",
			QueryType:        "within",
		},
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
				{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][]float64{{30, 20}, {30, 30}, {45, 66}},
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multipolygon does not cover multipoint",
			QueryType:        "within",
		},
		{
			QueryShape: [][][][]float64{{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
				{{{1, 0}, {2, 0}, {2, 1}, {1, 1}, {1, 0}}}},
			DocShapeVertices: [][]float64{{0.5, 0.5}, {1.5, 0.5}},
			DocShapeName:     "multipoint1",
			Expected:         []string{"multipoint1"},
			Desc:             "multiple multipolygons required to cover multipoint",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "multipoint", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
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

func TestMultiPolygonMultiLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
				{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][]float64{{{45, 40}, {10, 40}}, {{45, 40}, {10, 40}, {30, 20}}},
			DocShapeName:     "multilinestring1",
			Expected:         nil,
			Desc:             "multilinestring intersecting at the edge of multipolygon",
			QueryType:        "within",
		},
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
				{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][]float64{{{45, 40}, {10, 40}}, {{45, 40}, {10, 40}, {30, 12}}},
			DocShapeName:     "multilinestring1",
			Expected:         nil,
			Desc:             "multipolygon does not cover multilinestring",
			QueryType:        "within",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "multilinestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
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

func TestMultiPolygonWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape: [][][][]float64{{{{16, 6}, {41, 11}, {11, 21}, {6, 11}, {16, 6}}},
				{{{31, 21}, {46, 41}, {11, 41}, {31, 21}}}},
			DocShapeVertices: [][][][]float64{{{{31, 21}, {46, 41}, {11, 41}, {31, 21}}}},
			DocShapeName:     "multipolygon1",
			Expected:         []string{"multipolygon1"},
			Desc:             "multipolygon covers another multipolygon",
			QueryType:        "within",
		},
		{
			QueryShape: [][][][]float64{{{{16, 6}, {41, 11}, {11, 21}, {6, 11}, {16, 6}}},
				{{{31, 21}, {46, 41}, {11, 41}, {31, 21}}}},
			DocShapeVertices: [][][][]float64{{{{31, 21}, {46, 41}, {16, 46}, {31, 21}}}},
			DocShapeName:     "multipolygon1",
			Expected:         nil,
			Desc:             "multipolygon does not cover multipolygon",
			QueryType:        "within",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			test.DocShapeVertices, "multipolygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
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
