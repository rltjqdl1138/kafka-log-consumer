const HOST = "192.168.0.52:9092"

const fs = require('fs')
const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({kafkaHost:HOST});
const topic = 'logging.metaseoul.request';

const HOUR_MILL = 60 * 60 * 1000;
const DAY_MILL = 24 * 60 * 60 * 1000;
const GMT_PIVOT = 9 * 60* 60 * 1000;
const DAY_MAP = {}
function handleLog(msg){
  const {value} = msg;
  const now = new Date();

  try{
    const filename1 = `raw-${now.getFullYear()}.${now.getMonth()+1}.${now.getDate()}.log`
    fs.appendFileSync(`${__dirname}/${filename1}`, `\n${value}`);
    const jsonStr = JSON.parse(value);
    const {data, timestamp} = JSON.parse(jsonStr);

    const hourPivot = Math.floor((timestamp+GMT_PIVOT)/HOUR_MILL);
    const dayPivot = Math.floor((timestamp+GMT_PIVOT)/DAY_MILL) * 24;
    const hourStamp = hourPivot - dayPivot
    if(!DAY_MAP[dayPivot]){
      DAY_MAP[dayPivot] = {
        userList:[],
        hourList:[],
      };
    }
    if( !DAY_MAP[dayPivot].userList.includes(data.userSeq) )
      DAY_MAP[dayPivot].userList.push(data.userSeq);

    if(!DAY_MAP[dayPivot].hourList[hourStamp]){
      DAY_MAP[dayPivot].hourList[hourStamp] = [data.userSeq];
    }else if(!DAY_MAP[dayPivot].hourList.includes(data.userSeq)){
      DAY_MAP[dayPivot].hourList.push(data.userSeq);
    }

    Object.entries(DAY_MAP).forEach( ([key,val]) => printLog(key,val))

  }catch(e){
    console.log(e)
  }

}

function printLog(timestamp, {userList, hourList}){
  const a = parseInt(timestamp, 10);
  const date = new Date(a*HOUR_MILL);
  console.log(`  [${date.getFullYear()}.${date.getMonth()}.${date.getDate()}] DAU: ${userList.length}명`)
  console.log(userList)
  for(let i=0; i<24; i++){
    if(!hourList[i]) 
      console.log(`  * ${i}시:\t 0명`);
    else
      console.log(`  * ${i}시:\t ${hourList[i].length}명`);
  }
}


const topics = [{topic}]
const options = {
  autoCommit: false,
  autoCommitIntervalMs: 5000,
  fetchMaxWaitMs: 100,
  fetchMinBytes: 1,
  fetchMaxBytes: 1024 * 1024,
  fromOffset: false,
  encoding: 'utf8',
  keyEncoding: 'utf8'
}

const consumer = new Consumer(client, topics, options)

client.on('ready',()=>console.log('client ready'));

consumer.on('message', handleLog)


