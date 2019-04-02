Adcreativeid:Int 广告创意id（>=200000:dsp） 

Adplatformproviderid:Int 广告平台商id（>=100000:rtb）

Putinmodeltype:Int 根据广告主的投放模式，1：显示量投放，2：点击量投放

Requestmode:Int 数据请求方式（1：请求，2：展示，3：点击）

Requestdate:String 请求时间格式为：yyyy-m-dd hh:mm:ss
zf
Client:Int 设备类型（如：1：Android,2：IOS，3：wp）

Iseffective:Int 有效标识（有效指可以正常计费的）（0：无效，1：有效）

Isbilling:Int 是否收费（0：未收费，1：收费）

Adspacestype:Int 广告位类型（1：banner2：插屏3：全屏）

Devicetype:Int 设备类型（1：手机:2：平板）

Processnode:Int 流程节点（1：请求量ktp 2 :有效请求 3：广告请求）

Paymode:Int 针对平台商的支付模式1：展示量投放（CMP）2：点击

Megratype:Int媒体类型1：长尾媒体2：视频媒体3：独立媒体，默认：1

Isbid:Int  是否rtp [0:否,1:是]
Bidprice:Double Rtp竞价价格
Winprice:Double Rtp竞价成功价格
Iswin:Int 是否竞价成功


 指标                        定义

参与竞价数                   本日收到ADX发来的竞价请求并成功相应次数
竞价成功数                   在本日内成功竞价的次数

竞价成功率                   竞价成功率=竞价成功数/参与竞价数
展示量（曝光）                广告在终端被显示的数量

点击量                       广告展示后被终端用户点击的数量
点击率                       点击率=点击量/展示量

ECPC                        ECPC=成本/点击量
ECPM                        ECPM=成本/展示量*1000

消费                         收取广告主支付的用于广告投放的费用
成本                         广告花费在渠道与媒体上的费用
毛利                         毛利=消费-成本


