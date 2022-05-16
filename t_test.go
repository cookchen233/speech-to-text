package main

import (
	"flag"
	"os"
	"testing"
	"time"
)

func TestNewCallRecordRecognition(t *testing.T) {
	var begin string
	var end string

	td := time.Now().Format("2006-01-02")
	td_time, _ := time.ParseInLocation("2006-01-02", td, loc)
	tmr := td_time.Add(time.Second * 86400).Format("2006-01-02")
	flag.StringVar(&begin, "b", td, "开始日期, 格式: yyyy-mm-dd")
	flag.StringVar(&end, "e", tmr, "结束日期, 格式: yyyy-mm-dd")

	// 从arguments中解析注册的flag。必须在所有flag都注册好而未访问其值时执行。未注册却使用flag -help时，会返回ErrHelp。
	flag.Parse()

	begin_time, _ := time.ParseInLocation("2006-01-02", begin, loc)
	end_time, _ := time.ParseInLocation("2006-01-02", end, loc)
	if begin_time.Unix() < 0{
		pr("起始日格式不正确")
	}
	if end_time.Unix() < 0{
		pr("岂止日格式不正确")
	}
	if end != "" && end_time.Unix() <=  begin_time.Unix(){
		pr("岂止日必须大于起始日")
	}
	biz := "bx"
	c := NewCallRecordRecognition(biz, os.Getenv(biz+"_dir"), os.Getenv(biz+"_trans_dir"))
	day_dur, _ := c.day_duration(begin, end)
	pr(day_dur)
}