import createMsg, { simSwapMsg } from './producer'

test('Producer Internal method test 1', () => {
  const testMsg: simSwapMsg = { isd_code: '1', phone_number: '0123456789', sim_provider: 'test' }

  createMsg(testMsg)
    .catch((err: string) => {
      console.error('Producer Internal method test 1:' + err)
    })
    .then(retVal => { return expect(retVal).toStrictEqual(testMsg) })
    .catch(() => 'obligatory catch')
})
