package main

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"os"
	"sync"
)

var connections = make(map[string]*gorm.DB)
func GetDB(biz string)(*gorm.DB){
	_, ok := connections[biz]
	if ok {
		return connections[biz]
	}
	var err error
	var db *gorm.DB
	db, err  = gorm.Open("mysql", fmt.Sprintf("%s:%s@(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", os.Getenv("mysql_" + biz + "_user"), os.Getenv("mysql_" + biz + "_pass"), os.Getenv("mysql_" + biz + "_host"), os.Getenv("mysql_" + biz + "_db")))
	if err!= nil{
		panic(err)
	}
	db.SingularTable(true)
	db.LogMode(false)
	connections[biz] = db
	return db
}

type AsrResult struct {
	Id         int `gorm:"AUTO_INCREMENT"`
	Ctime     int64
	LocalFilenameMd5        string  `gorm:"type:varchar(50);unique_index"`
	LocalFilename         string  `gorm:"size:500"`
	AliFilename string `gorm:"type:varchar(500)"`
	AliSignFilename string `gorm:"type:varchar(500)"`
	ResultText      string  `gorm:type:longtext"`
	Biz         string  `gorm:"size:50"`
	CallTime         int64
	CallType         string  `gorm:"size:50"`
	CallId         string  `gorm:"size:50"`
	CrmUserId         string  `gorm:"size:50"`
	CallPhone         string  `gorm:"size:50"`
	TargetPhone         string  `gorm:"size:50"`
	Extra1         string  `gorm:"size:500"`
}