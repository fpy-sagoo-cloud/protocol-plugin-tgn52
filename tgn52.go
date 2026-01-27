//go:build linux

package main

import (
	"encoding/hex"
	"fmt"
	"net/rpc"
	"strings"
	"time"

	"gitee.com/fpy-go/hotgo-plugin-base/extend/model"
	"github.com/gogf/gf/v2/util/guid"

	plugin "gitee.com/fpy-go/hotgo-plugin-base/extend/module"
	gplugin "github.com/hashicorp/go-plugin"
)

// ProductKey hotgo平台产品标识，对应呼吸贴产品
const ProductKey = "202422012110346"

// ProductKeyNoMainDataTopic hotgo平台产品标识，对应呼吸贴产品非主数据topic
const ProductKeyNoMainDataTopic = "product.hxt.device.parse.no.main.data.receive"

// 因为数据包长度占用一个字节，所以最大为255，所以这里设置最大长度为255
const maxPacketLength = 255

const FrameStart = "FAFAFAAA"
const StartFrameMethodFunc = "start"

const FrameEnd = "FAAAAAAA"
const EndFrameMethodFunc = "end"

const FrameFlashFull = "FAFFFFAA"
const FlashFullFrameMethodFunc = "flashFull"

const BatteryFrameHeader = "FAFB"
const BatteryFrameMethodFunc = "battery"
const UpPropertyFrameMethodFunc = "upProperty"

const FrameMethod = "thing.event.command.post"

// ProtocolHxt 实现
type ProtocolHxt struct{}

func (p *ProtocolHxt) Info() model.ModuleInfo {
	var res = model.ModuleInfo{}
	res.Name = "hxt"
	res.Title = "呼吸贴v1设备协议"
	res.Author = "feng"
	res.Intro = "对呼吸贴设备进行数据采集v1"
	res.Version = "0.01"
	return res
}

func (p *ProtocolHxt) Encode(args interface{}) model.JsonRes {
	var resp model.JsonRes
	fmt.Println("接收到参数：", args)
	return resp
}

func (p *ProtocolHxt) Decode(data model.DataReq) model.JsonRes {
	var resp = &model.JsonRes{}
	resp.Code = 0

	if dataProcess(resp, data) {
		return *resp
	} else if commandProcess(resp, data) {
		return *resp
	} else if batteryProcess(resp, data) {
		return *resp
	} else {
		resp.Message = "数据解析失败"
	}

	return *resp
}

func batteryProcess(resp *model.JsonRes, data model.DataReq) bool {
	dataBytes := data.Data
	// 检查数据长度,最少四个字节
	if len(dataBytes) < 4 {
		resp.Code = 1
		resp.Message = "数据长度不足"
		return false
	}
	hexStr := strings.ToUpper(hex.EncodeToString(dataBytes[:2]))
	// 验证帧头是否为 FA
	if BatteryFrameHeader != hexStr {
		resp.Code = 1
		resp.Message = "电量帧头错误，期望 " + BatteryFrameHeader
		return false
	}

	if dataBytes[3] != 0xAA {
		resp.Code = 1
		resp.Message = "帧尾错误，期望 AA"
		return false
	}

	var rd = make(map[string]model.Param)
	nowTime := time.Now().Unix()
	rd[BatteryFrameMethodFunc] = model.Param{Value: fmt.Sprintf("%02X", dataBytes[2]), Time: nowTime}
	rd[ProductKey] = model.Param{Value: ProductKeyNoMainDataTopic, Time: nowTime}
	resp.Data = model.HotgoMqttModel{
		Id:            guid.S(),
		Version:       "1.0",
		Sys:           model.SysInfo{Ack: 0},
		Params:        rd,
		Method:        FrameMethod,
		ModelFuncName: BatteryFrameMethodFunc,
	}
	return true
}

func commandProcess(resp *model.JsonRes, data model.DataReq) bool {
	dataBytes := data.Data
	// 检查数据长度,最少四个字节
	if len(dataBytes) < 4 {
		resp.Code = 1
		resp.Message = "数据长度不足"
		return false
	}
	// 验证帧头是否为 FA
	if dataBytes[0] != 0xFA {
		resp.Code = 1
		resp.Message = "帧头错误，期望 FA"
		return false
	}

	var rd = make(map[string]model.Param)
	nowTime := time.Now().Unix()
	mqttModel := model.HotgoMqttModel{
		Id:            guid.S(),
		Version:       "1.0",
		Sys:           model.SysInfo{Ack: 0},
		Params:        rd,
		Method:        FrameMethod,
		ModelFuncName: "",
	}

	// 判断是否是整个测量数据开始数据帧
	hexStr := strings.ToUpper(hex.EncodeToString(dataBytes))
	if hexStr == FrameStart {
		mqttModel.ModelFuncName = StartFrameMethodFunc
		rd[mqttModel.ModelFuncName] = model.Param{Value: true, Time: nowTime}
		return true
	} else if hexStr == FrameEnd {
		mqttModel.ModelFuncName = EndFrameMethodFunc
		rd[mqttModel.ModelFuncName] = model.Param{Value: true, Time: nowTime}
		return true
	} else if hexStr == FrameFlashFull {
		mqttModel.ModelFuncName = FlashFullFrameMethodFunc
		rd[mqttModel.ModelFuncName] = model.Param{Value: true, Time: nowTime}
		return true
	}
	rd[ProductKey] = model.Param{Value: ProductKeyNoMainDataTopic, Time: nowTime}
	mqttModel.Params = rd
	return true
}

func dataProcess(resp *model.JsonRes, data model.DataReq) bool {
	dataBytes := data.Data
	// 检查数据长度是否足够（至少要有帧头、包序号、包长度、帧尾）
	if len(dataBytes) < 4 {
		resp.Code = 1
		resp.Message = "数据长度不足"
		return false
	}

	// 验证帧头是否为 FA
	if dataBytes[0] != 0xFA {
		resp.Code = 1
		resp.Message = "帧头错误，期望 FA"
		return false
	}

	// 验证帧尾是否在正确位置
	expectedLength := int(dataBytes[2]) // 包长度
	if expectedLength < 4 || expectedLength != len(dataBytes) {
		resp.Code = 1
		resp.Message = "包长度不匹配"
		return false
	}

	if dataBytes[expectedLength-1] != 0xAA {
		resp.Code = 1
		resp.Message = "帧尾错误，期望 AA"
		return false
	}

	// 提取各部分数据
	packetSequence := int(dataBytes[1])        // 包序号 (0-255 循环)
	packetLength := int(dataBytes[2])          // 包长度
	payload := dataBytes[3 : expectedLength-1] // 数据帧部分

	var rd = make(map[string]model.Param)
	nowTime := time.Now().Unix()

	// 设置解析结果
	rd["packetSequence"] = model.Param{Value: packetSequence, Time: nowTime}       // 包序号，特别注意这是关键字段
	rd["packetLength"] = model.Param{Value: packetLength, Time: nowTime}           // 包长度
	rd["payload"] = model.Param{Value: formatPayloadToHex(payload), Time: nowTime} // 数据帧部分
	rd["capacitance"] = model.Param{Value: convertBytesToIntArray(payload), Time: nowTime}
	if data.DataIdent != nil && data.DataIdent.UserKey != "" {
		rd["user"] = model.Param{Value: data.DataIdent.UserKey, Time: nowTime}
	} else {
		rd["user"] = model.Param{Value: "", Time: nowTime}
	}

	sequenceData := make([]int, 0)
	for i := 0; i < len(payload); i++ {
		// 创建序列数据
		sequenceData = append(sequenceData, packetSequence*maxPacketLength+i)
	}
	rd["sequence"] = model.Param{Value: sequenceData, Time: nowTime}

	resp.Code = 0
	resp.Data = model.HotgoMqttModel{
		Id:            guid.S(),
		Version:       "1.0",
		Sys:           model.SysInfo{Ack: 0},
		Params:        rd,
		Method:        FrameMethod,
		ModelFuncName: UpPropertyFrameMethodFunc,
	}
	return true
}

// 将字节数组转换为整数数组
func convertBytesToIntArray(dataBytes []byte) []int {
	result := make([]int, len(dataBytes))
	for i, b := range dataBytes {
		result[i] = int(b)
	}
	return result
}

// 辅助函数：将字节数组转换为十六进制字符串表示
func formatPayloadToHex(payload []byte) string {
	result := ""
	for i, b := range payload {
		if i > 0 {
			result += " "
		}
		result += fmt.Sprintf("%02X", b)
	}
	return result
}

// HxtPlugin 插件接口实现
// 这有两种方法：服务器必须为此插件返回RPC服务器类型。我们为此构建了一个RPCServer。
// 客户端必须返回我们的接口的实现通过RPC客户端。我们为此返回RPC。
type HxtPlugin struct{}

// Server 此方法由插件进程延迟调
func (t *HxtPlugin) Server(*gplugin.MuxBroker) (interface{}, error) {
	return &plugin.ProtocolRPCServer{Impl: new(ProtocolHxt)}, nil
}

// Client 此方法由宿主进程调用
func (t *HxtPlugin) Client(b *gplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &plugin.ProtocolRPC{Client: c}, nil
}

func main() {
	//调用plugin.Serve()启动侦听，并提供服务
	//ServeConfig 握手配置，插件进程和宿主机进程，都需要保持一致
	gplugin.Serve(&gplugin.ServeConfig{
		HandshakeConfig: plugin.HandshakeConfig,
		Plugins:         pluginMap,
	})
}

// 插件进程必须指定Impl，此处赋值为greeter对象
var pluginMap = map[string]gplugin.Plugin{
	"hxt": new(HxtPlugin),
}
