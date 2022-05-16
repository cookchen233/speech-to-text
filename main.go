package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	logrus_stack "github.com/Gurpartap/logrus-stack"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/joho/godotenv"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

//文件处理
type FileHandler struct {
	trans_dir string
	mu        sync.Mutex
}

func NewFileHandler(trans_dir string) FileHandler {
	return FileHandler{
		trans_dir: trans_dir,
	}
}

//转换音频文件比特率
func (bind *FileHandler) TransBitRate(filename string) (string, error) {
	dir := path.Join(bind.trans_dir, path.Base(path.Dir(filename)))
	trans_filename := path.Join(dir, path.Base(filename))
	if !IsDir(dir) {
		os.MkdirAll(dir, os.ModePerm)
	}
	cmd := exec.Command("ffmpeg", "-i", filename, "-ac", "1", "-ar", "8000", trans_filename, "-y")
	o, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("cmd.Run() failed with %s\n%s", err, o)
	}
	if FileSize(trans_filename) < 10 || !IsFile(trans_filename) {
		return "", errors.New("转码文件异常:" + strconv.FormatInt(FileSize(trans_filename), 10))
	}
	return trans_filename, nil
}

//阿里云
type Aliyun struct {
	oss_path   string
	access_key string
	secret     string
	app_key    string
	bucket     string
	endpoint   string
}

func NewAliyun(oss_path string, app_key string) Aliyun {
	return Aliyun{
		oss_path:   oss_path,
		access_key: os.Getenv("aliyun_access_key"),
		secret:     os.Getenv("aliyun_secret"),
		app_key:    app_key,
		bucket:     os.Getenv("aliyun_bucket"),
		endpoint:   os.Getenv("aliyun_endpoint"),
	}
}

//上传文件
func (bind *Aliyun) UploadFile(filename string) (string, string, error) {
	//time.Sleep(time.Duration(5) * time.Second)
	//return filename, filename, nil
	client, err := oss.New(bind.endpoint, bind.access_key, bind.secret)
	if err != nil {
		return "", "", err
	}
	bucket, err := client.Bucket(bind.bucket)
	if err != nil {
		return "", "", err
	}
	file_key := bind.oss_path + PartFilename(filename)
	err = bucket.PutObjectFromFile(file_key, filename)
	if err != nil {
		return "", "", err
	}
	url, _ := bucket.SignURL(file_key, "GET", 86400*365)
	//url := fmt.Sprintf("https://%s.%s/%s", bind.bucket, bind.endpoint, file_key), nil
	return file_key, strings.Replace(url, "-internal", "", 1), nil
}

//请求语音识别
func (bind *Aliyun) RequestASR(ali_filename string) (map[string]interface{}, string, error) {
	//time.Sleep(time.Duration(6) * time.Second)
	//return ali_filename, nil
	// 地域ID，固定值。
	const REGION_ID string = "cn-shanghai"
	const ENDPOINT_NAME string = "cn-shanghai"
	const PRODUCT string = "nls-filetrans"
	const DOMAIN string = "filetrans.cn-shanghai.aliyuncs.com"
	const API_VERSION string = "2018-08-17"
	const POST_REQUEST_ACTION string = "SubmitTask"
	const GET_REQUEST_ACTION string = "GetTaskResult"
	// 请求参数
	const KEY_APP_KEY string = "appkey"
	const KEY_FILE_LINK string = "file_link"
	const KEY_VERSION string = "version"
	const KEY_ENABLE_WORDS string = "enable_words"
	// 响应参数
	const KEY_TASK string = "Task"
	const KEY_TASK_ID string = "TaskId"
	const KEY_STATUS_TEXT string = "StatusText"
	const KEY_RESULT string = "Result"
	// 状态值
	const STATUS_SUCCESS string = "SUCCESS"
	const STATUS_RUNNING string = "RUNNING"
	const STATUS_QUEUEING string = "QUEUEING"
	var accessKeyId string = bind.access_key
	var accessKeySecret string = bind.secret
	var appKey string = bind.app_key
	var fileLink = ali_filename
	client, err := sdk.NewClientWithAccessKey(REGION_ID, accessKeyId, accessKeySecret)
	if err != nil {
		return map[string]interface{}{}, "", err
	}
	postRequest := requests.NewCommonRequest()
	postRequest.Domain = DOMAIN
	postRequest.Version = API_VERSION
	postRequest.Product = PRODUCT
	postRequest.ApiName = POST_REQUEST_ACTION
	postRequest.Method = "POST"
	mapTask := make(map[string]string)
	mapTask[KEY_APP_KEY] = appKey
	mapTask[KEY_FILE_LINK] = fileLink
	// 新接入请使用4.0版本，已接入（默认2.0）如需维持现状，请注释掉该参数设置。
	mapTask[KEY_VERSION] = "4.0"
	// 设置是否输出词信息，默认为false。开启时需要设置version为4.0。
	mapTask[KEY_ENABLE_WORDS] = "false"
	mapTask["auto_split"] = "true"
	task, err := json.Marshal(mapTask)
	if err != nil {
		return map[string]interface{}{}, "", err
	}
	postRequest.FormParams[KEY_TASK] = string(task)
	postResponse, err := client.ProcessCommonRequest(postRequest)
	if err != nil {
		return map[string]interface{}{}, "", err
	}
	postResponseContent := postResponse.GetHttpContentString()
	if postResponse.GetHttpStatus() != 200 {
		return map[string]interface{}{}, postResponseContent, fmt.Errorf("识别请求失败，Http错误码: %d", postResponse.GetHttpStatus())
	}
	var postMapResult map[string]interface{}
	err = json.Unmarshal([]byte(postResponseContent), &postMapResult)
	if err != nil {
		return map[string]interface{}{}, postResponseContent, err
	}
	var taskId string = ""
	var statusText string = ""
	statusText = postMapResult[KEY_STATUS_TEXT].(string)
	if statusText == STATUS_SUCCESS {
		taskId = postMapResult[KEY_TASK_ID].(string)
	} else {
		return postMapResult, postResponseContent, fmt.Errorf("识别请求失败, 返回内容: %s", postResponseContent)
	}
	getRequest := requests.NewCommonRequest()
	getRequest.Domain = DOMAIN
	getRequest.Version = API_VERSION
	getRequest.Product = PRODUCT
	getRequest.ApiName = GET_REQUEST_ACTION
	getRequest.Method = "GET"
	getRequest.QueryParams[KEY_TASK_ID] = taskId
	statusText = ""
	var getResponseContent string
	var getMapResult map[string]interface{}
	for true {
		getResponse, err := client.ProcessCommonRequest(getRequest)
		if err != nil {
			return map[string]interface{}{}, "", err
		}
		getResponseContent = getResponse.GetHttpContentString()
		if getResponse.GetHttpStatus() != 200 {
			return map[string]interface{}{}, getResponseContent, fmt.Errorf("查询请求失败，Http错误码：%d", getResponse.GetHttpStatus())
		}
		err = json.Unmarshal([]byte(getResponseContent), &getMapResult)
		if err != nil {
			return map[string]interface{}{}, getResponseContent, err
		}
		statusText = getMapResult[KEY_STATUS_TEXT].(string)
		if statusText == STATUS_RUNNING || statusText == STATUS_QUEUEING {
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	if statusText == STATUS_SUCCESS {
		return getMapResult, getResponseContent, nil
	} else {
		return getMapResult, getResponseContent, fmt.Errorf("识别失败，返回内容：%s", getResponseContent)
	}
}

//获取语音识别结果
func (bind *Aliyun) GetASRResult(task_id string) (string, error) {
	return gmd5(task_id), nil
}

//处理调度
type CallRecordRecognition struct {
	root_dir        string
	biz             string
	aliyun          Aliyun
	file_handler    FileHandler
	handle_num      int
	save_limit_chan chan struct{}
	mu              sync.Mutex
	gl              Glimit
	first_tip       int
	safe_go         PanicGroup
}

func NewCallRecordRecognition(biz string, root_dir string, trans_dir string) CallRecordRecognition{
	handle_num, _ := strconv.Atoi(os.Getenv("handle_num"))
	return CallRecordRecognition{
		root_dir:        root_dir,
		biz:             biz,
		aliyun:          NewAliyun("call-record/"+biz+"/", os.Getenv("aliyun_"+biz+"_app_key")),
		file_handler:    NewFileHandler(trans_dir),
		handle_num:      handle_num,
		save_limit_chan: make(chan struct{}, 50),
		gl:              NewGlimit(handle_num),
		safe_go:         NewSafeGo(handle_num),
	}
}
func (bind *CallRecordRecognition) run() {
	file_maxsize, _ := strconv.Atoi(os.Getenv("file_maxsize"))
	//filesize_segment, _ := strconv.Atoi(os.Getenv("filesize_segment"))
	//for _, max_size := range grange(1024*1024, 1024*1024*file_maxsize, 1024*1024*filesize_segment){
	//	bind.HandleDirList(max_size)
	//}
	if _, ok := bind.IsReachTodayLimit(); ok {
		return
	}
	bind.HandleDirList(file_maxsize)
}
func (bind *CallRecordRecognition) ExecLock(fn interface{}, args ...interface{}) {
	bind.mu.Lock()
	defer func() { bind.mu.Unlock() }()
	v := reflect.ValueOf(fn)
	rargs := make([]reflect.Value, len(args))
	for i, a := range args {
		rargs[i] = reflect.ValueOf(a)
	}
	v.Call(rargs)
}

//统计录音时长
func (bind *CallRecordRecognition) CountDuration(asr_results []AsrResult) float64 {
	var dur_count float64
	for _, v := range asr_results {
		var mapResult map[string]interface{}
		err := json.Unmarshal([]byte(v.ResultText), &mapResult)
		if err != nil {
			log.Error("json错误 :", err, v.ResultText, ", id:", v.Id)
		} else {
			dur_count += mapResult["BizDuration"].(float64)
		}
	}
	return dur_count
}

//查询日期范围已处理时长
func (bind *CallRecordRecognition) DayDuration(begin string, end string) (float64, error) {
	begin_time, _ := time.ParseInLocation("2006-01-02", begin, loc)
	end_time, _ := time.ParseInLocation("2006-01-02", end, loc)
	var asr_results []AsrResult
	result := GetDB(bind.biz).Where("ctime BETWEEN ? AND ? AND extra1 = ''", begin_time.Unix(), end_time.Unix()).Find(&asr_results)
	if result.RowsAffected > 0 {
		return bind.CountDuration(asr_results), nil
	}
	if result.Error != nil && !result.RecordNotFound() {
		log.Error("查询出错", result.Error)
		return 0, result.Error
	}
	return 0, nil
}

//判断当日处理时长是否已达到设定限制
func (bind *CallRecordRecognition) IsReachTodayLimit() (float64, bool) {
	limit_day_dur, _ := strconv.ParseFloat(os.Getenv("day_dur"), 64)
	limit_day_dur *= 3600000
	td := time.Now().Format("2006-01-02")
	td_time, _ := time.ParseInLocation("2006-01-02", td, loc)
	tmr := td_time.Add(time.Second * 86400).Format("2006-01-02")
	day_dur, err := bind.DayDuration(td, tmr)
	if err != nil {
		log.Error("统计时长出错", err)
		return 0, false
	}
	day_dur_time, _ := time.ParseDuration(fmt.Sprintf("%.0fms", day_dur))
	limit_day_dur_time, _ := time.ParseDuration(fmt.Sprintf("%.0fms", limit_day_dur))
	if day_dur >= limit_day_dur {
		log.Warnf("今日处理录音时长已达%s, 每日最大限制为%s, 不再处理", day_dur_time, limit_day_dur_time)
		return day_dur, true
	}
	return day_dur, false
}

//处理文件
func (bind *CallRecordRecognition) HandleFile(filename string) {
	day_dur, ok := bind.IsReachTodayLimit()
	if ok {
		return
	}
	a := time.Now()
	part_filename := PartFilename(filename)
	if FileSize(filename) < 10{
		log.Warn("文件小于10B, 忽略, ", filename)
		ca.Set(gmd5("hanled_file_"+bind.biz+part_filename), 1, ne)
		return
	}
	var asr_result AsrResult
	var filename_info = [7]string{}
	arr := strings.Split(path.Base(filename), `-`)
	for k := range filename_info {
		if len(arr) > k {
			filename_info[k] = arr[k]
		}
	}
	var call_time int64 = 0
	if filename_info[0] != "" {
		x := strings.Split(filename_info[0], ".")[0]
		if len(x) != 10 {
			//ymnas/YM/clink/voice/20160817/3003990-20160817103030-18939945468-075548982211-record-1-1471401029.33210.mp3
			//log.Warn("文件名不符合规则, 忽略, ", filename)
			//return
			filename_info[1] = filename_info[5]
		}
		call_time, _ = strconv.ParseInt(x, 10, 64)
	}
	if filename_info[1] == "" {
		filename_info[1] = "0"
	}

	result := GetDB(bind.biz).Where("local_filename_md5 = ?", gmd5(bind.biz+part_filename)).Limit(1).Find(&asr_result)
	if result.RowsAffected > 0 {
		log.Warn("已处理过的文件, 忽略, ", filename)
		ca.Set(gmd5("hanled_file_"+bind.biz+part_filename), 1, ne)
		return
	}
	if result.Error != nil && !result.RecordNotFound() {
		log.Error("查询出错", result.Error)
		return
	}
	trans_filename, err := bind.file_handler.TransBitRate(filename)
	if err != nil {
		log.Error("转码失败, ", filename, err)
		return
	}
	bind.ExecLock(func(s string) {
		if bind.first_tip < 1 {
			bind.first_tip++
			pr("首次转码成功, ", s)
		}
	}, trans_filename)
	ali_filename, ali_sign_filename, err := bind.aliyun.UploadFile(trans_filename)
	if err != nil {
		log.Error("上传失败, ", filename, err)
		return
	}
	ca.Set(gmd5("hanled_file_"+bind.biz+part_filename), 1, ne)
	bind.ExecLock(func(s string) {
		if bind.first_tip < 2 {
			bind.first_tip++
			pr("首次上传成功, ", s)
		}
	}, ali_sign_filename)
	os.Remove(trans_filename)
	request_asr_result, response_text, err := bind.aliyun.RequestASR(ali_sign_filename)
	extra1 := ""
	if err != nil {
		log.Error("语音识别失败, ", filename, err)
		response_text = fmt.Sprintf("%s", err)
		extra1 = "fail4"
		//return
	}
	bind.ExecLock(func(s string) {
		if bind.first_tip < 3 {
			bind.first_tip++
			pr("首次识别成功, ", s)
		}
	}, response_text)

	data := AsrResult{
		Ctime:            time.Now().Unix(),
		LocalFilenameMd5: gmd5(bind.biz + part_filename),
		LocalFilename:    part_filename,
		AliFilename:      ali_filename,
		AliSignFilename:  ali_sign_filename,
		ResultText:       response_text,
		Biz:              bind.biz,
		CallTime:         call_time,
		CallType:         filename_info[1],
		CallId:           filename_info[2],
		CrmUserId:        filename_info[3],
		CallPhone:        filename_info[4],
		TargetPhone:      filename_info[5],
		Extra1:           extra1,
	}
	ca.Set(gmd5("hanled_file_"+bind.biz+part_filename), 1, ne)
	bind.save_limit_chan <- struct{}{}
	defer func() { <-bind.save_limit_chan }()
	if err := GetDB(bind.biz).Create(&data).Error; err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") {
			log.Warn("重复入库, 忽略, ", filename, err)
		} else {
			log.Error("入库失败, ", filename, err)
			return
		}
	} else {
		bind.ExecLock(func(s string) {
			if bind.first_tip < 4 {
				bind.first_tip++
				pr("首次入库成功, ", s)
			}
		}, data.LocalFilename)
	}
	bind.ExecLock(func() {
		handled_num++
	})
	dur_time, _ := time.ParseDuration(fmt.Sprintf("%.0fms", request_asr_result["BizDuration"].(float64)))
	day_dur_time, _ := time.ParseDuration(fmt.Sprintf("%.0fms", day_dur + request_asr_result["BizDuration"].(float64)))
	log.Info("文件处理完成, ", filename,
		"\n计数:", handled_num, ", 文件体积:", ByteCountBinary(FileSize(filename)), ", 用时:", time.Now().Sub(a), ", 录音时长:", dur_time,
		"\n今日已处理录音总时长:", day_dur_time,
		"\n程序已运行时间:", time.Now().Sub(time_a))
}

//扫描目录
func (bind *CallRecordRecognition) HandleDirList(max_size int) {
	dirs, _ := ioutil.ReadDir(bind.root_dir)
	for _, dir := range dirs {
		d_key := gmd5("handled_dir_" + bind.biz + dir.Name())

		if path.Base(dir.Name()) == path.Ext(dir.Name()) {
			continue
		}
		if _, found := ca.Get(d_key); found {
			continue
		}
		file_list, _ := ioutil.ReadDir(path.Join(bind.root_dir, dir.Name()))
		file_count := len(file_list)
		ignore := 0
		for _, file := range file_list {
			part_filename := path.Join(dir.Name(), file.Name())
			if path.Base(file.Name()) == path.Ext(file.Name()) {
				ignore++
				continue
			}
			if int(file.Size()) > max_size {
				//wait ++
				//continue
			}
			if _, found := ca.Get(gmd5("hanled_file_" + bind.biz + part_filename)); found {
				ignore++
				continue
			}
			//bind.gl.Run(bind.HandleFile, path.Join(bind.root_dir, part_filename))
			bind.safe_go.Go(bind.HandleFile, path.Join(bind.root_dir, part_filename))
		}
		log.Info(bind.biz, " 已投放任务, ", dir.Name(), ", 总文件数:", file_count, ", 忽略数:", ignore) //, ", 延后处理:", wait)
		if ignore == file_count {
			ca.Set(d_key, 1, ne)
		}
	}
	bind.safe_go.Wait()
}

var handled_num int
var ca *cache.Cache
var ne = cache.NoExpiration
var loc, _ = time.LoadLocation("Asia/Shanghai")
var time_a time.Time

func init() {
	godotenv.Load()
	cache_file := "cache.gob"
	_, err := os.Lstat(cache_file)
	var M map[string]cache.Item
	if !os.IsNotExist(err) {
		File, _ := os.Open(cache_file)
		D := gob.NewDecoder(File)
		D.Decode(&M)
	}
	if len(M) > 0 {
		ca = cache.NewFrom(cache.NoExpiration, 10*time.Minute, M)
	} else {
		ca = cache.New(cache.NoExpiration, 10*time.Minute)
	}
	go func() {
		for {
			time.Sleep(time.Duration(60) * time.Second)
			File, _ := os.OpenFile(cache_file, os.O_RDWR|os.O_CREATE, 0777)
			defer File.Close()
			enc := gob.NewEncoder(File)
			if err := enc.Encode(ca.Items()); err != nil {
				log.Error(err)
			}
		}
	}()

	log.SetLevel(log.InfoLevel)

	callerLevels := []log.Level{
		log.PanicLevel,
		log.FatalLevel,
		log.ErrorLevel,
	}
	stackLevels := []log.Level{log.PanicLevel, log.FatalLevel, log.ErrorLevel}

	log.AddHook(logrus_stack.NewHook(callerLevels, stackLevels))

	log.AddHook(RotateLogHook("log", "stdout.log", 7*24*time.Hour, 24*time.Hour))

	if os.Getenv("environment") == "pro" {
		log.AddHook(&MailHook{
			os.Getenv("mail_user"),
			os.Getenv("mail_pass"),
			os.Getenv("mail_host"),
			os.Getenv("mail_port"),
			strings.Split(os.Getenv("err_receivers"), ";"),
		})
	}

}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("发生错误", err)
			time.Sleep(time.Second * 600)
			main()
		}
	}()
	for {
		time_a = time.Now()
		handled_num = 0
		log.Info("开始处理")
		bizs := strings.Split(os.Getenv("bizs"), `,`)
		safe_go := NewSafeGo(10)
		for _, biz := range bizs {
			safe_go.Go(func(biz string) {
				c := NewCallRecordRecognition(biz, os.Getenv(biz+"_dir"), os.Getenv(biz+"_trans_dir"))
				c.run()
			}, biz)
		}
		safe_go.Wait()
		next, _ := time.ParseDuration("3600s")
		log.Info("全部处理完成, 用时:", time.Now().Sub(time_a), ", 下次执行时间:", time.Now().Add(next).Format("2006-01-02 15:04:05"))
		time.Sleep(time.Duration(3600) * time.Second)

	}

}
