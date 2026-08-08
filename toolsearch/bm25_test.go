package toolsearch

import (
	"reflect"
	"sync"
	"testing"
)

func TestTokenize(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		text string
		want []string
	}{
		{name: "ASCII", text: "Fetch_Post-DETAIL v2", want: []string{"fetch", "post", "detail", "v2"}},
		{name: "CJK bigrams", text: "热榜数据", want: []string{"热榜", "榜数", "数据"}},
		{name: "single Han", text: "榜", want: []string{"榜"}},
		{name: "mixed", text: "TikTok用户 profile", want: []string{"tiktok", "用户", "profile"}},
		{name: "punctuation", text: " _-!? ", want: nil},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := Tokenize(test.text); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("Tokenize(%q) = %#v, want %#v", test.text, got, test.want)
			}
		})
	}
}

func TestNewRejectsInvalidDocuments(t *testing.T) {
	t.Parallel()
	for _, documents := range [][]Document{
		{{ID: "", Text: "search"}},
		{{ID: " search", Text: "search"}},
		{{ID: "search", Text: " "}},
		{{ID: "search", Text: "_!?"}},
		{{ID: "search", Text: "first"}, {ID: "search", Text: "second"}},
	} {
		if _, err := New(documents); err == nil {
			t.Fatalf("New(%#v) succeeded, want error", documents)
		}
	}
}

func TestSearchRankingAndDeterminism(t *testing.T) {
	t.Parallel()
	documents := []Document{
		{ID: "video_detail", Text: "video detail 视频详情 item id"},
		{ID: "video_search", Text: "search videos 视频搜索 keyword"},
		{ID: "user_videos", Text: "user post video list 用户作品列表"},
	}
	index, err := New(documents)
	if err != nil {
		t.Fatal(err)
	}
	got := index.Search("视频详情", 3)
	if len(got) < 1 || got[0].ID != "video_detail" || got[0].Score <= 0 {
		t.Fatalf("Search() = %#v, want video_detail ranked first", got)
	}
	first := index.Search("user video list", 3)
	for i := 0; i < 100; i++ {
		if got := index.Search("user video list", 3); !reflect.DeepEqual(got, first) {
			t.Fatalf("search %d drifted: %#v != %#v", i, got, first)
		}
	}
}

func TestSearchIndependentOfCatalogOrder(t *testing.T) {
	t.Parallel()
	documents := []Document{
		{ID: "a_tool", Text: "shared search"},
		{ID: "b_tool", Text: "shared search"},
		{ID: "c_tool", Text: "other"},
	}
	reversed := []Document{documents[2], documents[1], documents[0]}
	left, err := New(documents)
	if err != nil {
		t.Fatal(err)
	}
	right, err := New(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := left.Search("shared", 3), right.Search("shared", 3); !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog order changed ranking: %#v != %#v", got, want)
	}
}

func TestIndexConcurrentSearch(t *testing.T) {
	t.Parallel()
	index, err := New([]Document{
		{ID: "search_posts", Text: "search posts 搜索帖子 keyword"},
		{ID: "read_post", Text: "read post 读取帖子 id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := index.Search("搜索帖子", 2)
	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < 100; j++ {
				if got := index.Search("搜索帖子", 2); !reflect.DeepEqual(got, want) {
					t.Errorf("concurrent search drifted: %#v != %#v", got, want)
					return
				}
			}
		}()
	}
	wait.Wait()
}

func TestSearchEdgeCases(t *testing.T) {
	t.Parallel()
	index, err := New([]Document{{ID: "search", Text: "search query"}})
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string][]Hit{
		"empty query":   index.Search("", 1),
		"zero limit":    index.Search("search", 0),
		"unknown query": index.Search("missing", 1),
		"nil index":     (*Index)(nil).Search("search", 1),
	} {
		if got != nil {
			t.Errorf("%s = %#v, want nil", name, got)
		}
	}
}
