package cn.dmp.beans

import com.dmp.utils.NBUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


class Log(val sessionid: String,
          val advertisersid: Int,
          val adorderid: Int,
          val adcreativeid: Int,
          val adplatformproviderid: Int,
          val sdkversion: String,
          val adplatformkey: String,
          val putinmodeltype: Int,
          val requestmode: Int,
          val adprice: Double,
          val adppprice: Double,
          val requestdate: String,
          val ip: String,
          val appid: String,
          val appname: String,
          val uuid: String,
          val device: String,
          val client: Int,
          val osversion: String,
          val density: String,
          val pw: Int,
          val ph: Int,
          val long: String,
          val lat: String,
          val provincename: String,
          val cityname: String,
          val ispid: Int,
          val ispname: String,
          val networkmannerid: Int,
          val networkmannername: String,
          val iseffective: Int,
          val isbilling: Int,
          val adspacetype: Int,
          val adspacetypename: String,
          val devicetype: Int,
          val processnode: Int,
          val apptype: Int,
          val district: String,
          val paymode: Int,
          val isbid: Int,
          val bidprice: Double,
          val winprice: Double,
          val iswin: Int,
          val cur: String,
          val rate: Double,
          val cnywinprice: Double,
          val imei: String,
          val mac: String,
          val idfa: String,
          val openudid: String,
          val androidid: String,
          val rtbprovince: String,
          val rtbcity: String,
          val rtbdistrict: String,
          val rtbstreet: String,
          val storeurl: String,
          val realip: String,
          val isqualityapp: Int,
          val bidfloor: Double,
          val aw: Int,
          val ah: Int,
          val imeimd5: String,
          val macmd5: String,
          val idfamd5: String,
          val openudidmd5: String,
          val androididmd5: String,
          val imeisha1: String,
          val macsha1: String,
          val idfasha1: String,
          val openudidsha1: String,
          val androididsha1: String,
          val uuidunknow: String,
          val userid: String,
          val iptype: Int,
          val initbidprice: Double,
          val adpayment: Double,
          val agentrate: Double,
          val lomarkrate: Double,
          val adxrate: Double,
          val title: String,
          val keywords: String,
          val tagid: String,
          val callbackdate: String,
          val channelid: String,
          val mediatype: Int) extends Product with Serializable {

  // 角标和成员属性的映射关系
  override def productElement(n: Int): Any = n match {
    case 0 => sessionid
    case 1 => advertisersid
    case 2 => adorderid
    case 3 => adcreativeid
    case 4 => adplatformproviderid
    case 5 => sdkversion
    case 6 => adplatformkey
    case 7 => putinmodeltype
    case 8 => requestmode
    case 9 => adprice
    case 10 => adppprice
    case 11 => requestdate
    case 12 => ip
    case 13 => appid
    case 14 => appname
    case 15 => uuid
    case 16 => device
    case 17 => client
    case 18 => osversion
    case 19 => density
    case 20 => pw
    case 21 => ph
    case 22 => long
    case 23 => lat
    case 24 => provincename
    case 25 => cityname
    case 26 => ispid
    case 27 => ispname
    case 28 => networkmannerid
    case 29 => networkmannername
    case 30 => iseffective
    case 31 => isbilling
    case 32 => adspacetype
    case 33 => adspacetypename
    case 34 => devicetype
    case 35 => processnode
    case 36 => apptype
    case 37 => district
    case 38 => paymode
    case 39 => isbid
    case 40 => bidprice
    case 41 => winprice
    case 42 => iswin
    case 43 => cur
    case 44 => rate
    case 45 => cnywinprice
    case 46 => imei
    case 47 => mac
    case 48 => idfa
    case 49 => openudid
    case 50 => androidid
    case 51 => rtbprovince
    case 52 => rtbcity
    case 53 => rtbdistrict
    case 54 => rtbstreet
    case 55 => storeurl
    case 56 => realip
    case 57 => isqualityapp
    case 58 => bidfloor
    case 59 => aw
    case 60 => ah
    case 61 => imeimd5
    case 62 => macmd5
    case 63 => idfamd5
    case 64 => openudidmd5
    case 65 => androididmd5
    case 66 => imeisha1
    case 67 => macsha1
    case 68 => idfasha1
    case 69 => openudidsha1
    case 70 => androididsha1
    case 71 => uuidunknow
    case 72 => userid
    case 73 => iptype
    case 74 => initbidprice
    case 75 => adpayment
    case 76 => agentrate
    case 77 => lomarkrate
    case 78 => adxrate
    case 79 => title
    case 80 => keywords
    case 81 => tagid
    case 82 => callbackdate
    case 83 => channelid
    case 84 => mediatype
  }

  // 对象一个又多少个成员属性
  override def productArity: Int = 85

  // 比较两个对象是否是同一个对象
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Log]


  override def toString = s"$sessionid,$advertisersid,$adorderid,$adcreativeid,$adplatformproviderid,$sdkversion,$adplatformkey,$putinmodeltype,$requestmode,$adprice,$adppprice,$requestdate,$ip,$appid,$appname,$uuid,$device,$client,$osversion,$density,$pw,$ph,$long,$lat,$provincename,$cityname,$ispid,$ispname,$networkmannerid,$networkmannername,$iseffective,$isbilling,$adspacetype,$adspacetypename,$devicetype,$processnode,$apptype,$district,$paymode,$isbid,$bidprice,$winprice,$iswin,$cur,$rate,$cnywinprice,$imei,$mac,$idfa,$openudid,$androidid,$rtbprovince,$rtbcity,$rtbdistrict,$rtbstreet,$storeurl,$realip,$isqualityapp,$bidfloor,$aw,$ah,$imeimd5,$macmd5,$idfamd5,$openudidmd5,$androididmd5,$imeisha1,$macsha1,$idfasha1,$openudidsha1,$androididsha1,$uuidunknow,$userid,$iptype,$initbidprice,$adpayment,$agentrate,$lomarkrate,$adxrate,$title,$keywords,$tagid,$callbackdate,$channelid,$mediatype\n"
}


object Log {
  def apply(arr: Array[String]): Log = new Log(
    arr(0),
    NBUtil.toInt(arr(1)),
    NBUtil.toInt(arr(2)),
    NBUtil.toInt(arr(3)),
    NBUtil.toInt(arr(4)),
    arr(5),
    arr(6),
    NBUtil.toInt(arr(7)),
    NBUtil.toInt(arr(8)),
    NBUtil.toDouble(arr(9)),
    NBUtil.toDouble(arr(10)),
    arr(11),
    arr(12),
    arr(13),
    arr(14),
    arr(15),
    arr(16),
    NBUtil.toInt(arr(17)),
    arr(18),
    arr(19),
    NBUtil.toInt(arr(20)),
    NBUtil.toInt(arr(21)),
    arr(22),
    arr(23),
    arr(24),
    arr(25),
    NBUtil.toInt(arr(26)),
    arr(27),
    NBUtil.toInt(arr(28)),
    arr(29),
    NBUtil.toInt(arr(30)),
    NBUtil.toInt(arr(31)),
    NBUtil.toInt(arr(32)),
    arr(33),
    NBUtil.toInt(arr(34)),
    NBUtil.toInt(arr(35)),
    NBUtil.toInt(arr(36)),
    arr(37),
    NBUtil.toInt(arr(38)),
    NBUtil.toInt(arr(39)),
    NBUtil.toDouble(arr(40)),
    NBUtil.toDouble(arr(41)),
    NBUtil.toInt(arr(42)),
    arr(43),
    NBUtil.toDouble(arr(44)),
    NBUtil.toDouble(arr(45)),
    arr(46),
    arr(47),
    arr(48),
    arr(49),
    arr(50),
    arr(51),
    arr(52),
    arr(53),
    arr(54),
    arr(55),
    arr(56),
    NBUtil.toInt(arr(57)),
    NBUtil.toDouble(arr(58)),
    NBUtil.toInt(arr(59)),
    NBUtil.toInt(arr(60)),
    arr(61),
    arr(62),
    arr(63),
    arr(64),
    arr(65),
    arr(66),
    arr(67),
    arr(68),
    arr(69),
    arr(70),
    arr(71),
    arr(72),
    NBUtil.toInt(arr(73)),
    NBUtil.toDouble(arr(74)),
    NBUtil.toDouble(arr(75)),
    NBUtil.toDouble(arr(76)),
    NBUtil.toDouble(arr(77)),
    NBUtil.toDouble(arr(78)),
    arr(79),
    arr(80),
    arr(81),
    arr(82),
    arr(83),
    NBUtil.toInt(arr(84))
  )
  def apply(arr: Row): Log = new Log(
    arr(0).toString,
    NBUtil.toInt(arr(1).toString),
    NBUtil.toInt(arr(2).toString),
    NBUtil.toInt(arr(3).toString),
    NBUtil.toInt(arr(4).toString),
    arr(5).toString,
    arr(6).toString,
    NBUtil.toInt(arr(7).toString),
    NBUtil.toInt(arr(8).toString),
    NBUtil.toDouble(arr(9).toString),
    NBUtil.toDouble(arr(10).toString),
    arr(11).toString,
    arr(12).toString,
    arr(13).toString,
    arr(14).toString,
    arr(15).toString,
    arr(16).toString,
    NBUtil.toInt(arr(17).toString),
    arr(18).toString,
    arr(19).toString,
    NBUtil.toInt(arr(20).toString),
    NBUtil.toInt(arr(21).toString),
    arr(22).toString,
    arr(23).toString,
    arr(24).toString,
    arr(25).toString,
    NBUtil.toInt(arr(26).toString),
    arr(27).toString,
    NBUtil.toInt(arr(28).toString),
    arr(29).toString,
    NBUtil.toInt(arr(30).toString),
    NBUtil.toInt(arr(31).toString),
    NBUtil.toInt(arr(32).toString),
    arr(33).toString,
    NBUtil.toInt(arr(34).toString),
    NBUtil.toInt(arr(35).toString),
    NBUtil.toInt(arr(36).toString),
    arr(37).toString,
    NBUtil.toInt(arr(38).toString),
    NBUtil.toInt(arr(39).toString),
    NBUtil.toDouble(arr(40).toString),
    NBUtil.toDouble(arr(41).toString),
    NBUtil.toInt(arr(42).toString),
    arr(43).toString,
    NBUtil.toDouble(arr(44).toString),
    NBUtil.toDouble(arr(45).toString),
    arr(46).toString,
    arr(47).toString,
    arr(48).toString,
    arr(49).toString,
    arr(50).toString,
    arr(51).toString,
    arr(52).toString,
    arr(53).toString,
    arr(54).toString,
    arr(55).toString,
    arr(56).toString,
    NBUtil.toInt(arr(57).toString),
    NBUtil.toDouble(arr(58).toString),
    NBUtil.toInt(arr(59).toString),
    NBUtil.toInt(arr(60).toString),
    arr(61).toString,
    arr(62).toString,
    arr(63).toString,
    arr(64).toString,
    arr(65).toString,
    arr(66).toString,
    arr(67).toString,
    arr(68).toString,
    arr(69).toString,
    arr(70).toString,
    arr(71).toString,
    arr(72).toString,
    NBUtil.toInt(arr(73).toString),
    NBUtil.toDouble(arr(74).toString),
    NBUtil.toDouble(arr(75).toString),
    NBUtil.toDouble(arr(76).toString),
    NBUtil.toDouble(arr(77).toString),
    NBUtil.toDouble(arr(78).toString),
    arr(79).toString,
    arr(80).toString,
    arr(81).toString,
    arr(82).toString,
    arr(83).toString,
    NBUtil.toInt(arr(84).toString)
  )
}