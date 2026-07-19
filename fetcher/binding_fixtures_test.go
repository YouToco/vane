package fetcher

// 本文件的三个样本自被删除的 tikhub_test.go / xhs_user_test.go / x_test.go 原样迁入
// （实测响应结构，绑定引擎的等价对照与单测共用——契约 §6.2/§8.2）。

const sampleTikhubResponse = `{
  "code": 200,
  "data": {
    "success": true,
    "msg": null,
    "data": {
      "items": [
        {
          "model_type": "note",
          "note": {
            "id": "69ca2af0000000001b020a10",
            "title": "分享几个AI创业方向",
            "desc": "去年开始有创业的想法…",
            "timestamp": 1783670775,
            "xsec_token": "ABtoken=",
            "user": {"nickname": "Zimablue"}
          }
        },
        {
          "model_type": "recommend_query",
          "note": null
        },
        {
          "model_type": "note",
          "note": {
            "id": "",
            "title": "空 id 应被跳过",
            "desc": "x",
            "timestamp": 0,
            "xsec_token": "",
            "user": {"nickname": ""}
          }
        }
      ]
    }
  }
}`

const sampleXHSUserResponse = `{
  "code": 200,
  "data": {
    "code": 0,
    "success": true,
    "msg": "success",
    "data": {
      "notes": [
        {
          "id": "6a5a501d000000000f03235e",
          "title": "AI编程，先别急着付费",
          "display_title": "AI编程，先别急着付费",
          "desc": "建议初学者先使用免费AI编程工具，积累经验后再考虑付费工具。",
          "create_time": 1784303645,
          "type": "normal",
          "user": {"userid": "6a5578b3000000000e03cc00", "nickname": "青木"}
        },
        {
          "id": "6a5a23680000000011006ef3",
          "title": "",
          "display_title": "零基础AI编程的三个核心步骤",
          "desc": "第一步理解需求，第二步拆解模块，第三步交给AI实现。",
          "create_time": 1784292200,
          "type": "video",
          "user": {"userid": "6a5578b3000000000e03cc00", "nickname": "青木"}
        }
      ],
      "tags": [],
      "has_more": false
    }
  }
}`

const sampleTwitterResponse = `{
  "code": 200,
  "data": {
    "status": "ok",
    "pinned": [
      {"tweet_id": "pin1", "text": "pinned tweet", "created_at": "Thu Jul 10 12:00:00 +0000 2026", "author": {"screen_name": "OpenAI"}}
    ],
    "timeline": [
      {
        "tweet_id": "t1",
        "text": "We are releasing GPT-5 today. Check it out!",
        "created_at": "Wed Jul 15 17:30:00 +0000 2026",
        "conversation_id": "t1",
        "views": "93400",
        "author": {"screen_name": "OpenAI", "name": "OpenAI", "rest_id": "123"},
        "media": [],
        "entities": []
      },
      {
        "tweet_id": "t2",
        "text": "RT @claudeai: We're introducing Claude for Te...",
        "created_at": "Wed Jul 15 16:00:00 +0000 2026",
        "conversation_id": "t2",
        "retweeted": true,
        "retweeted_tweet": {
          "tweet_id": "rt_orig_1",
          "text": "We're introducing Claude for Teachers — a free tool designed to help educators bring AI into the classroom responsibly.",
          "created_at": "Wed Jul 15 14:00:00 +0000 2026",
          "author": {"screen_name": "claudeai", "name": "Claude", "rest_id": "456"},
          "media": {},
          "entities": {}
        },
        "author": {"screen_name": "AnthropicAI", "name": "Anthropic", "rest_id": "789"},
        "media": [],
        "entities": []
      },
      {
        "tweet_id": "t3",
        "text": "This is a great analysis of our latest paper.",
        "created_at": "Wed Jul 15 15:00:00 +0000 2026",
        "quoted": {
          "tweet_id": "q1",
          "text": "Original quoted tweet text"
        },
        "author": {"screen_name": "OpenAI"},
        "media": [],
        "entities": []
      },
      {
        "tweet_id": "t4",
        "text": "More details in this thread...",
        "created_at": "Wed Jul 15 17:30:01 +0000 2026",
        "reply_to": "t1",
        "author": {"screen_name": "OpenAI"},
        "media": [],
        "entities": []
      }
    ]
  }
}`

// sampleHotListResponse 取自 2026-07-18 真实响应（裁剪 3 条）：条目无时间戳，updated_at 是坏值（契约 §7）。
const sampleHotListResponse = `{
 "code": 200,
 "data": {
  "date": "2026-07-19",
  "updated_at": "2026-06-07",
  "items": [
   {
    "rank": 1,
    "title": "用万能旅行拍照姿势美美出片",
    "url": "https://www.xiaohongshu.com/search_result?keyword=%E7%94%A8%E4%B8%87%E8%83%BD%E6%97%85%E8%A1%8C%E6%8B%8D%E7%85%A7%E5%A7%BF%E5%8A%BF%E7%BE%8E%E7%BE%8E%E5%87%BA%E7%89%87&type=51",
    "item_id": "231139944",
    "hot": "918.6w",
    "trend": "flat"
   },
   {
    "rank": 2,
    "title": "耗时三年拍下古诗词里的中国",
    "url": "https://www.xiaohongshu.com/search_result?keyword=%E8%80%97%E6%97%B6%E4%B8%89%E5%B9%B4%E6%8B%8D%E4%B8%8B%E5%8F%A4%E8%AF%97%E8%AF%8D%E9%87%8C%E7%9A%84%E4%B8%AD%E5%9B%BD&type=51",
    "item_id": "230858183",
    "hot": "907w",
    "trend": "flat"
   },
   {
    "rank": 3,
    "title": "我拍到了海鸥雨",
    "url": "https://www.xiaohongshu.com/search_result?keyword=%E6%88%91%E6%8B%8D%E5%88%B0%E4%BA%86%E6%B5%B7%E9%B8%A5%E9%9B%A8&type=51",
    "item_id": "231141374",
    "hot": "887.5w",
    "trend": "flat"
   }
  ]
 }
}`


// sampleTopicFeedResponse 取自 2026-07-18 真实响应（裁剪 3 条）：create_time 毫秒、sort=time 降序。
const sampleTopicFeedResponse = `{
 "code": 200,
 "data": {
  "success": true,
  "msg": null,
  "data": {
   "items": [
    {
     "id": "6a573dde000000000f0292df",
     "title": "三年忍辱，换我两万五月薪",
     "desc": "#都市职场[话题]#  #逆袭[话题]#  #爽文[话题]#  #女性成长[话题",
     "create_time": 1784102366000,
     "user": {
      "user_id": "69bfda630000000034019ee8",
      "nickname": "雪代千律"
     }
    },
    {
     "id": "6a545174000000001101cee4",
     "title": "都来接好运",
     "desc": "私企打工人踩坑三年，来回导线上负15 W ，利西越滚越头疼。\n好在后来我再网上说",
     "create_time": 1783910772000,
     "user": {
      "user_id": "631165ae0000000015019e48",
      "nickname": "阿达西瓜"
     }
    },
    {
     "id": "6a4f66f8000000001101ca69",
     "title": "就业迷茫？打破信息差，大胆冲车载测试",
     "desc": "在家待业、反复失业是不是越待越焦虑😥\n干体力活薪资低没前景，投简历清一色销售流水",
     "create_time": 1783588600000,
     "user": {
      "user_id": "630e20500000000012001b03",
      "nickname": "重庆市八品职业培训学校"
     }
    }
   ]
  }
 }
}`


// sampleFavedNotesResponse 取自 2026-07-18 真实响应（裁剪 3 条）：create_time 秒、序列非单调（收藏序≠创建序）。
const sampleFavedNotesResponse = `{
 "code": 200,
 "data": {
  "success": true,
  "msg": null,
  "data": {
   "notes": [
    {
     "id": "68f6d58d0000000004021c58",
     "title": "当你骗他，你的亲密行为是别人教的",
     "display_title": "当你骗他，你的亲密行为是别人教的",
     "desc": "#bg[话题]# #短文[话题]# #占有欲男主[话题]# #偏执[话题]# #",
     "create_time": 1761006989,
     "user": {
      "nickname": "X"
     }
    },
    {
     "id": "68c58262000000001d008c0d",
     "title": "年上｜点进看熟男竹马生闷气",
     "display_title": "年上｜点进看熟男竹马生闷气",
     "desc": "#bg[话题]# #做梦素材[话题]# #原创短篇小说[话题]# #原创小说[话",
     "create_time": 1757774434,
     "user": {
      "nickname": "有一点饿"
     }
    },
    {
     "id": "6a3e53dc000000000f0295be",
     "title": "何必一厢旧梦悠悠苦我心",
     "display_title": "何必一厢旧梦悠悠苦我心",
     "desc": "那些年我直播画过的一小时摸鱼头们，发个小合集hhh#画画[话题]##绘画过程[话",
     "create_time": 1782469596,
     "user": {
      "nickname": "tho肉肉（九月有课）"
     }
    }
   ],
   "has_more": true
  }
 }
}`

