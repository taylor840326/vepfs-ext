package vepfsext

import (
	"fmt"
	"os"

	"github.com/volcengine/volcengine-go-sdk/service/vepfs"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/request"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
)

// Used for custom request initialization logic
var initRequest func(*request.Request)

type CreateLensTaskRequest struct {
	// 数据洞察任务的名称
	LensTaskName string `json:"LensTaskName"`
	// 关联的数据洞察策略ID
	LensPolicyId string `json:"LensPolicyId"`
	// 关联的文件系统ID
	FileSystemId string `json:"FileSystemId"`
	// 数据洞察任务的执行目录
	// 数据洞察任务的执行目录可为文件系统中的Fileset子目录或指定目录
	// 此参数取值为Fileset子目录或指定目录的绝对路径
	TargetPath string `json:"TargetPath"`
	// 数据洞察任务的描述信息
	Description string `json:"Description"`
	// 数据洞察任务的类型。
	// 取值说明如下：
	// MetadataExport : 导出任务。以获取任务执行结果文件为目标的任务为导出任务
	// MetadataAnalyze: 分析任务。以根据任务执行结果获取分析信息为目标的任务为分析任务。
	LensTaskContent []string `json:"LensTaskContent"`
	// 导出任务详情
	LensExportInfo LensExportInfo `json:"LensExportInfo"`
	// 分析任务详情
	LensAnalysisInfo LensAnalysisInfo `json:"LensAnalysisInfo"`
	// 任务的执行目录信息。任务的执行范围将被限制在此目录内。
	TargetInfos []LensTargetInfo `json:"TargetInfos"`
}

type LensExportInfo struct {
	// 任务执行结果导出的目标目录的绝对路径
	ExportPath string `json:"ExportPath"`
	// 导出的文件属性
	ExportAttrs []string `json:"ExportAttrs"`
	// 一级目录容量查询。取值说明如下：
	// true: 开启一级目录容量查询功能
	// false: 关闭一级目录容量查询功能
	FirstLevelSubDir bool `json:"FirstLevelSubDir"`
	// 二级目录容量查询。取值说明如下：
	// true: 开启二级目录容量查询功能
	// false: 关闭二级目录容量查询功能
	SecondLevelSubDir bool `json:"SecondLevelSubDir"`
	// 导出的目标TOS桶名称
	TosBucket string `json:"TosBucket"`
	// 导出的目标TOS桶路径前缀
	TosPrefix string `json:"TosPrefix"`
	// 是否导出任务执行结果到控制台。取值范围如下：
	// true: 导出到控制台
	// false：不导出到控制台
	EnableDownload bool `json:"EnableDownload"`
}

type LensAnalysisInfo struct {
	// 是否开启任务执行结果分析功能。取值说明如下：
	// true: 开启
	// false: 关闭
	// LensTaskContent 参数取值中包含MetadataAnalyze时，取值为true，否则取值为false
	EnableLensAnalysis bool `json:"EnableLensAnalysis"`
	// 分析的文件属性
	AnalysisAttrs []string `json:"AnalysisAttrs"`
}

type LensTargetInfo struct {
	// 数据洞察任务执行的Fileset ID。取值说明如下：
	// 为空: 数据洞察任务的执行目录非Fileset
	// 不为空：数据洞察任务的执行目录为Fileset，本参数取值为Fileset ID
	FilesetId string `json:"FilesetId"`
	// 任务的执行目录，根据FilesetId参数。本参数存在以下2个取值情况：
	// FilesetId 参数取值为空，表示执行目录为文件系统中的指定目录，本参数取值为执行目录的绝对路径。
	// FilesetId 参数取值不为空。 表示执行目录为Fileset子目录，本参数取值为执行目录相对Fileset的相对路径
	RelativePath string `json:"RelativePath"`
}

type CreateLensTaskResponse struct {
	LensTaskId string `json:"LensTaskId"`
}

type VEPFSExt struct {
	vepfs.VEPFS
}

func NewVEPFSExt() *VEPFSExt {
	var (
		ak     string
		sk     string
		region string
		config *volcengine.Config
		sess   *session.Session
		err    error
	)

	ak = os.Getenv("VOLCENGINE_ACCESS_KEY_ID")
	sk = os.Getenv("VOLCENGINE_ACCESS_KEY_SECRET")
	region = os.Getenv("VOLCENGINE_REGION")
	config = volcengine.NewConfig().WithCredentials(credentials.NewStaticCredentials(ak, sk, "")).WithRegion(region)
	sess, err = session.NewSession(config)
	if err != nil {
		fmt.Printf("Failed to create session, err: %v\n", err)
		os.Exit(1)
	}

	client := vepfs.New(sess)
	return &VEPFSExt{
		VEPFS: *client,
	}
}

func (e *VEPFSExt) CreateLensTask(input *CreateLensTaskRequest) (*CreateLensTaskResponse, error) {
	req, out := e.CreateLensTaskInner(input)
	return out, req.Send()
}

func (e *VEPFSExt) CreateLensTaskInner(input *CreateLensTaskRequest) (req *request.Request, output *CreateLensTaskResponse) {
	op := &request.Operation{
		Name:       "CreateLensTasks",
		HTTPMethod: "POST",
		HTTPPath:   "/",
	}

	if input == nil {
		input = &CreateLensTaskRequest{}
	}

	output = &CreateLensTaskResponse{}
	req = e.VEPFS.NewRequest(op, input, output)
	// Run custom request initialization if present
	if initRequest != nil {
		initRequest(req)
	}
	req.HTTPRequest.Header.Set("Content-Type", "application/json; charset=utf-8")

	return
}
