require('dotenv').config({ path: '.env.local' })
// console.log(process.env) // remove this

const Seneca = require('seneca')

const { default: weaviate } = require('weaviate-client')



// run()

direct()

async function direct() {
  const client = await weaviate.connectToWCS(
    process.env.SENECA_WEAVIATE_TEST_CLUSTERURL, {
      authCredentials: new weaviate.ApiKey(process.env.SENECA_WEAVIATE_TEST_ADMINKEY),
      headers: { 'X-OpenAI-Api-Key': process.env.SENECA_OPENAI_KEY }
    } 
  )
  
  // console.log(client)
  /*
  const c02c = await client.collections.create({
    name: 'c02',
    vectorizer: 'text2vec-openai',
    properties: [
      {
        name: 'chunk',
        dataType: 'text',
      }
    ],
  })

  console.log('c0cg', c02c)
  */

  
  const c02g = await client.collections.get({name:'c02'})

  // console.log('c02g', c02g)


  const d01 = await c02g.data.insert({
    properties: {
      chunk: 'd01a',
    },
  })

  console.log(d01)
}



async function run() {
  const seneca = Seneca({ legacy: false })
    .test()
    .use('promisify')
        .use('entity')
  /*
    .use('..', {
      map: {
        'foo/chunk': '*',
      },
      index: {
        exact: process.env.SENECA_WEAVIATE_TEST_INDEX,
      },
      weaviate: {
        node: process.env.SENECA_WEAVIATE_TEST_NODE,
      },
    })
*/
  await seneca.ready()

  // console.log(await seneca.entity('bar/qaz').data$({q:1}).save$())

  /*
  const save0 = await seneca.entity('foo/chunk')
        .make$()
        .data$({
          x:3,
          o:{m:'M2',n:3}, 
          text: 't03',
          vector: [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.6],
          directive$:{vector$:true},
        })
        .save$()
  console.log('save0', save0)
  */

  // const id = '1%3A0%3Au0rACY4BB33NxQZdwDrQ'
  // const id = 'notanid'
  //const id = '1%3A0%3AvUrfCY4BB33NxQZd-DrZ'
  const id = '1%3A0%3AvUrfCY4BB33NxQZd-DrQ'
  const load0 = await seneca.entity('foo/chunk').load$(id)
  console.log('load0', load0)

  /*
  const list0 = await seneca.entity('foo/chunk').list$({
    // x:2
    directive$:{vector$:true},
    vector:[0.1,0.1,0.2,0.3,0.4,0.5,0.6,0.7],
  })
  console.log('list0', list0)


  console.log(await seneca.entity('bar/qaz').list$())
  */
}
