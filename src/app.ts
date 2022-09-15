import express from 'express'
import createMsg from './producer/producer'

const app = express()
const port = 3000
// below line is critical for http json requests
app.use(express.json())

app.get('/', (req, res) => {
  res.send('Hello World!')
})

app.listen(port, () => {
  return console.log(`Express is listening at http://localhost:${port}`)
})

app.post('/producer', (req, res) => {
  createMsg(req.body).then((retVal) => res.send(JSON.stringify(retVal)))
    .catch((err: string) => {
      console.error(`Something went wrong:\n${err}`)
    })
})
