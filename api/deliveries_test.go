package api

import (
	"net/url"
	"testing"
)

// TestParseHistoryQuery 参数解析：缺省、透传、越界与非数字回人话错误。
func TestParseHistoryQuery(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		wantSize  int
		wantToken string
		wantErr   bool
	}{
		{"全缺省", "", 0, "", false},
		{"空值按缺省", "page_size=&page_token=", 0, "", false},
		{"正常透传", "page_size=50&page_token=abc", 50, "abc", false},
		{"下边界", "page_size=1", 1, "", false},
		{"上边界", "page_size=100", 100, "", false},
		{"零越界", "page_size=0", 0, "", true},
		{"负数越界", "page_size=-1", 0, "", true},
		{"超上限", "page_size=101", 0, "", true},
		{"非数字", "page_size=abc", 0, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, _ := url.ParseQuery(c.raw)
			got, errMsg := parseHistoryQuery(q)
			if c.wantErr {
				if errMsg == "" {
					t.Fatalf("期望报错，得 errMsg 空")
				}
				return
			}
			if errMsg != "" {
				t.Fatalf("意外报错: %s", errMsg)
			}
			if got.PageSize != c.wantSize || got.PageToken != c.wantToken {
				t.Errorf("得 {size:%d token:%q}，期望 {size:%d token:%q}",
					got.PageSize, got.PageToken, c.wantSize, c.wantToken)
			}
		})
	}
}
