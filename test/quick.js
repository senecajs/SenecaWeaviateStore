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
    name: 'C09',
    // vectorizer: 'text2vec-openai',
    vectorizer: weaviate.configure.vectorizer.text2VecOpenAI(),
    properties: [
      {
        name: 'chunk',
        dataType: 'text',
        },
      {
        name: 'owner',
        dataType: 'text',
      }
    ],
  })

  console.log('c0cg', c04c)
*/
  
  
  // Const c04g = await client.collections.get({name:'c04'})
  const c04g = await client.collections.get('C11')

  console.log('c04g', c04g)


  const cols = await client.collections.listAll()
  console.log('cols', cols.map(c=>c.name))
  
  /*
  const d01 = await c04g.data.insert({
    properties: {
      chunk: 'd01b',
    },
  })

  console.log('INSERT', d01)
  */

  /*
  const list = await c04g.query.nearText(['d01b'], {
    limit: 2,
    returnProperties: [
      'chunk',
      // 'owner',
    ],
  })

  console.dir(list,{depth:null})
  */

  
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
          collection: {
            C10: {
              config: ()=>({
                vectorizer: weaviate.configure.vectorizer.text2VecOpenAI(),
                properties: [
                  {
                    name: 'chunk',
                    dataType: 'text',
                  },
                  {
                    name: 'owner',
                    dataType: 'text',
                  }
                ],
              })
            }
          },
          url: process.env.SENECA_WEAVIATE_TEST_CLUSTERURL,
          client: {
            authCredentials: new weaviate.ApiKey(process.env.SENECA_WEAVIATE_TEST_ADMINKEY),
            headers: { 'X-OpenAI-Api-Key': process.env.SENECA_OPENAI_KEY }
          },
        })

  await seneca.ready()

/*  
  const save0 = await seneca.entity('foo/chunk')
        .make$()
        .data$({
          x:3,
          o:{m:'M2',n:3}, 
          text: 't03',
          chunk: 'abc',
          owner: 'a'
        })
        .save$()
  console.log('save0', save0)

  const load0 = await seneca.entity('foo/chunk').load$(save0.id)
  console.log('load0', load0)

  
  const save1 = await seneca.entity('foo/chunk')
        .make$()
        .data$({
          chunk: 'abd',
          owner: 'b'
        })
        .save$()
  console.log('save1', save1)

  const load1 = await seneca.entity('foo/chunk').load$(save1.id)
  console.log('load1', load1)
*/
  
  const list0 = await seneca.entity('foo/chunk').list$({
    chunk:'abc'
  })
  console.log('list0', list0)


  const list1 = await seneca.entity('foo/chunk').list$({
    chunk:'abe'
  })
  console.log('list1', list1)


  const list2 = await seneca.entity('foo/chunk').list$({
    chunk:'abe',
    owner: 'a',
  })
  console.log('list2', list2)

  
}
