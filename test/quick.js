require('dotenv').config({ path: '.env.local' })
// console.log(process.env) // remove this

const Seneca = require('seneca')

const { default: weaviate } = require('weaviate-client')



run()

// direct()

async function direct() {
  const client = await weaviate.connectToWCS(
    process.env.SENECA_WEAVIATE_TEST_CLUSTERURL, {
      authCredentials: new weaviate.ApiKey(process.env.SENECA_WEAVIATE_TEST_ADMINKEY),
      headers: { 'X-OpenAI-Api-Key': process.env.SENECA_OPENAI_KEY }
    } 
  )

  console.log(client)

  /*
  const c04c = await client.collections.create({
    name: 'C04',
    // vectorizer: 'text2vec-openai',
    vectorizer: weaviate.configure.vectorizer.text2VecOpenAI(),
    properties: [
      {
        name: 'chunk',
        dataType: 'text',
      }
    ],
  })

  console.log('c0cg', c04c)
  */
  
  
  // Const c04g = await client.collections.get({name:'c04'})
  const c04g = await client.collections.get('C04')

  // console.log('c04g', c04g)


  const d01 = await c04g.data.insert({
    properties: {
      chunk: 'd01b',
    },
  })

  console.log(d01)


  const list = await c04g.query.nearText(['d01a'], {
    limit: 2,
    returnProperties: ['chunk'],
  })

  console.dir(list,{depth:null})
}



async function run() {
  const seneca = Seneca({ legacy: false })
        .test()
        .use('promisify')
        .use('entity')
        .use('..', {
          map: {
            'foo/chunk': '*',
          },
          collection: {},
          weaviate: {
          },
        })

  await seneca.ready()

  const save0 = await seneca.entity('foo/chunk')
        .make$()
        .data$({
          x:3,
          o:{m:'M2',n:3}, 
          text: 't03',
          vector: 'abc'
        })
        .save$()
  console.log('save0', save0)

  const id = save0.id
  const load0 = await seneca.entity('foo/chunk').load$(id)
  console.log('load0', load0)

  const list0 = await seneca.entity('foo/chunk').list$({
    vector:'abc'
  })
  console.log('list0', list0)

  const list1 = await seneca.entity('foo/chunk').list$({
    vector:'abd'
  })
  console.log('list1', list1)

}
