import Kafka from 'node-rdkafka'
import * as confconfig from '../confluent-properties.json'

function createConfigMap(config: any): object {
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

async function createProducer(onDeliveryReport: any): Promise<Kafka.Producer> {
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

export default async function createMsg(inmsgValue: simSwapMsg): Promise<simSwapMsg> {
  const topic = 'SIMSWAP'
  console.log(inmsgValue)
  const defValue = { isd_code: '0', phone_number: '0123456789', sim_provider: 'test' }
  const msgValue = { ...defValue, ...inmsgValue }

  // console.log(`default msg ${defValue}: inmsg = ${inmsgValue} resultant = ${msgValue}`)
  console.log('default msg ' + JSON.stringify(defValue) +
    ': inmsg = ' + JSON.stringify(inmsgValue) +
    ' resultant = ' + JSON.stringify(msgValue))

  const onDelReport = (err: string, report: any): any => {
    if (err === '') {
      console.warn('Error producing', err)
    } else {
      console.log(report)
      const { topic, key, value } = report
      const k = key.toString().padEnd(10, ' ')
      console.log(`Produced event to topic ${String(topic)}: key = ${String(k)} value = ${String(value)}`)
    }
  }

  const producer = await createProducer(onDelReport)

  const key = msgValue.isd_code + msgValue.phone_number
  const value = Buffer.from(JSON.stringify(msgValue))

  await producer.produce(topic, -1, value, key)

  producer.flush(10000, () => {
    producer.disconnect()
  })

  return msgValue
}
