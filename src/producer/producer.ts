import Kafka from 'node-rdkafka'
import * as confconfig from '../confluent-properties.json'

function createConfigMap (config: any): object {
  if (Object.prototype.hasOwnProperty.call(config, 'security.protocol')) {
    return {
      'bootstrap.servers': config['bootstrap.servers'],
      'sasl.username': config['sasl.username'],
      'sasl.password': config['sasl.password'],
      'security.protocol': config['security.protocol'],
      'sasl.mechanisms': config['sasl.mechanisms'],
      dr_msg_cb: true
    }
  } else {
    return {
      'bootstrap.servers': config['bootstrap.servers'],
      dr_msg_cb: true
    }
  }
}

async function createProducer (onDeliveryReport: any): Promise<Kafka.Producer> {
  const confconfig1 = createConfigMap(confconfig)
  // console.log(confconfig1)

  const producer = new Kafka.Producer(confconfig1)

  return await new Promise((resolve, reject) => {
    producer
      .on('ready', () => resolve(producer))
      .on('delivery-report', onDeliveryReport)
      .on('event.error', (err) => {
        console.warn('event.error', err)
        reject(err)
      })
    producer.connect()
  })
}

export interface simSwapMsg {
  isd_code: string
  phone_number: string
  sim_provider: string
}

const onDelReport = (err: string, report: any): void => {
  // -----
  // console.log() calls result in below exceptions during tests
  // **** Cannot log after tests are done. Did you forget to wait for something async in your test?
  // however tests pass as well as code runs fine without any problem
  // -----
  if (typeof err !== 'undefined' && err !== null && err !== '') {
    console.warn('Error producing', err)
  } else {
    // console.log(report)
    // const { topic, key, value } = report
    // const k = key.toString()
    // console.log(`Produced event to topic ${String(topic)}: key = ${String(k)} value = ${String(value)}`)
  }
}

export default async function createMsg (msgValue: simSwapMsg): Promise<simSwapMsg | null> {
  const topic = 'SIMSWAP'
  console.log(' input = ' + JSON.stringify(msgValue))

  const key = msgValue.isd_code + msgValue.phone_number
  const value = Buffer.from(JSON.stringify(msgValue))

  const producer = await createProducer(onDelReport)
  const ismsgcreated = producer.produce(topic, -1, value, key)
  // console.log('produce result:' + String(iscreated))

  producer.flush(10000, () => {
    producer.disconnect()
  })

  return (ismsgcreated === true) ? msgValue : null
}
