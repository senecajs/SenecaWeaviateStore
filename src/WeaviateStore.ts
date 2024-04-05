/* Copyright (c) 2024 Seneca contributors, MIT License */

const { default: Weaviate } = require('weaviate-client')

import { Gubu } from 'gubu'
import { StringDecoder } from 'string_decoder'

const { Child, Any } = Gubu

type Options = {
  debug: boolean
  map?: any
  field: {
    zone: { name: string }
    base: { name: string }
    name: { name: string }
    vector: { name: string }
  },
  url: string,
  client: any
  collection: Record<string, any>,
}

export type WeaviateStoreOptions = Partial<Options>


function WeaviateStore(this: any, options: Options) {
  const seneca: any = this

  const init = seneca.export('entity/init')

  const colmap: Record<string, any> = {}

  let client: any
  let desc: any = 'WeaviateStore'


  Object.entries(options.collection).map((entry: any[]) => {
    entry[1].name = entry[1].name || entry[0]
    entry[1].key = entry[0]
  })


  let store = {
    name: 'WeaviateStore',

    save: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent

      const canon = ent.canon$({ object: true })
      const coldef = resolveColDef(ent, options)
      const col = getCollection(coldef, ent)

      const body = ent.data$(false)

      // TODO: move to entity as a util?
      const fieldOpts: any = options.field

        ;['zone', 'base', 'name'].forEach((n: string) => {
          if ('' != fieldOpts[n].name && null != canon[n] && '' != canon[n]) {
            body[fieldOpts[n].name] = canon[n]
          }
        })

      const req = {
        index,
        body,
      }

      client
        .index(req)
        .then((res: any) => {
          const body = res.body
          ent.data$(body._source)
          ent.id = body._id
          reply(ent)
        })
        .catch((err: any) => reply(err))
    },

    load: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent

      // const canon = ent.canon$({ object: true })
      const index = resolveIndex(ent, options)

      let q = msg.q || {}

      if (null != q.id) {
        client
          .get({
            index,
            id: q.id,
          })
          .then((res: any) => {
            const body = res.body
            ent.data$(body._source)
            ent.id = body._id
            reply(ent)
          })
          .catch((err: any) => {
            // Not found
            if (err.meta && 404 === err.meta.statusCode) {
              reply(null)
            }

            reply(err)
          })
      } else {
        reply()
      }
    },

    list: function (msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent

      const index = resolveIndex(ent, options)
      const query = buildQuery({ index, options, msg })

      // console.log('LISTQ')
      // console.dir(query, { depth: null })

      if (null == query) {
        return reply([])
      }

      client
        .search(query)
        .then((res: any) => {
          const hits = res.body.hits
          const list = hits.hits.map((entry: any) => {
            let item = ent.make$().data$(entry._source)
            item.id = entry._id
            item.custom$ = { score: entry._score }
            return item
          })
          reply(list)
        })
        .catch((err: any) => {
          reply(err)
        })
    },

    // NOTE: all$:true is REQUIRED for deleteByQuery
    remove: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent

      const index = resolveIndex(ent, options)

      const q = msg.q || {}
      let id = q.id
      let query

      if (null == id) {
        query = buildQuery({ index, options, msg })

        if (null == query || true !== q.all$) {
          return reply(null)
        }
      }

      // console.log('REMOVE', id)
      // console.dir(query, { depth: null })

      if (null != id) {
        client
          .delete({
            index,
            id,
            // refresh: true,
          })
          .then((_res: any) => {
            reply(null)
          })
          .catch((err: any) => {
            // Not found
            if (err.meta && 404 === err.meta.statusCode) {
              return reply(null)
            }

            reply(err)
          })
      } else if (null != query && true === q.all$) {
        client
          .deleteByQuery({
            index,
            body: {
              query,
            },
            // refresh: true,
          })
          .then((_res: any) => {
            reply(null)
          })
          .catch((err: any) => {
            // console.log('REM ERR', err)
            reply(err)
          })
      } else {
        reply(null)
      }
    },

    close: function (this: any, _msg: any, reply: any) {
      this.log.debug('close', desc)
      reply()
    },

    // TODO: obsolete - remove from seneca entity
    native: function (this: any, _msg: any, reply: any) {
      reply(null, {
        client: () => client,
      })
    },
  }

  let meta = init(seneca, options, store)

  desc = meta.desc

  seneca.prepare(async function (this: any) {
    client = await Weaviate.connectToWCS(
      options.url,
      options.client
    )

    const coldefs = Object.values(options.collection)
    for (let cdI = 0; cdI < coldefs.length; cdI++) {
      const coldef = coldefs[cdI]

      let col

      try {
        col = await client.collections.get(coldef.name)
      }
      catch (e: any) {
        seneca.log.warn('collection-not-exist', { name: coldef.name })
      }

      if (null == col) {
        col = await client.collections.create({
          name: coldef.name,
          ...coldef.config
        })
      }

      colmap[coldef.key] = col
    }
  })


  function getCollection(coldef: any, ent: any) {
    if (null === coldef) {
      seneca.fail('no-collection-defined', { ent })
    }

    // options.collection might use entity canon as key - not a valid weaviate collection name
    const key = coldef.key
    const name = coldef.name
    const col = colmap[key] || colmap[name]

    if (null === col) {
      seneca.fail('no-collection-found', { key, name, ent })
    }

    return col
  }


  return {
    name: store.name,
    tag: meta.tag,
    exportmap: {
      native: () => {
        return { client }
      },
    },
  }
}


function resolveColDef(ent: any, options: Options) {
  let coldef = options.collection.default
  if (null != coldef) {
    return coldef
  }

  let canonstr = ent.canon$({ string: true })
  let coldefmap = options.collection
  if (null != coldefmap[canonstr]) {
    return coldefmap[canonstr]
  }

  return null
}



// Default options.
const defaults = {
  debug: false,
  map: Any(),
  // '' === name => do not inject
  field: {
    zone: { name: 'zone' },
    base: { name: 'base' },
    name: { name: 'name' },
    vector: { name: 'vector' },
  },
  url: String,
  client: {},
  collection: Child({}),
}

Object.assign(WeaviateStore, {
  defaults,
  utils: { resolveColDef },
})

export default WeaviateStore

if ('undefined' !== typeof module) {
  module.exports = WeaviateStore
}
