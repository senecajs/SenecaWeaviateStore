/* Copyright (c) 2024 Seneca contributors, MIT License */

import { AwsSigv4Signer } from '@weaviate-project/weaviate/aws'
import { Client } from '@weaviate-project/weaviate'
import { defaultProvider } from '@aws-sdk/credential-provider-node'

import { Gubu } from 'gubu'

const { Open, Any } = Gubu

type Options = {
  debug: boolean
  map?: any
  index: {
    prefix: string
    suffix: string
    map: Record<string, string>
    exact: string
  }
  field: {
    zone: { name: string }
    base: { name: string }
    name: { name: string }
    vector: { name: string }
  }
  cmd: {
    list: {
      size: number
    }
  }
  aws: any
  weaviate: any
}

export type WeaviateStoreOptions = Partial<Options>

function WeaviateStore(this: any, options: Options) {
  const seneca: any = this

  const init = seneca.export('entity/init')

  let desc: any = 'WeaviateStore'

  let client: any

  let store = {
    name: 'WeaviateStore',

    save: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent

      const canon = ent.canon$({ object: true })
      const index = resolveIndex(ent, options)

      const body = ent.data$(false)

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
    const region = options.aws.region
    const node = options.weaviate.node

    client = new Client({
      ...AwsSigv4Signer({
        region,
        service: 'aoss',
        getCredentials: () => {
          const credentialsProvider = defaultProvider()
          return credentialsProvider()
        },
      }),
      node,
    })
  })

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

function buildQuery(spec: { index: string; options: any; msg: any }) {
  const { index, options, msg } = spec

  const q = msg.q || {}

  let query: any = {
    index,
    body: {
      size: msg.size$ || options.cmd.list.size,
      _source: {
        excludes: [options.field.vector.name].filter((n) => '' !== n),
      },
      query: {},
    },
  }

  let excludeKeys: any = { vector: 1 }

  const parts = []

  for (let k in q) {
    if (!excludeKeys[k] && !k.match(/\$/)) {
      parts.push({
        match: { [k]: q[k] },
      })
    }
  }

  const vector$ = msg.vector$ || q.directive$?.vector$
  if (vector$) {
    parts.push({
      knn: {
        vector: {
          vector: q.vector,
          k: null == vector$.k ? 11 : vector$.k,
        },
      },
    })
  }

  if (0 === parts.length) {
    query = null
  } else if (1 === parts.length) {
    query.body.query = parts[0]
  } else {
    query.body.query = {
      bool: {
        must: parts,
      },
    }
  }

  return query
}

function resolveIndex(ent: any, options: Options) {
  let indexOpts = options.index
  if ('' != indexOpts.exact && null != indexOpts.exact) {
    return indexOpts.exact
  }

  let canonstr = ent.canon$({ string: true })
  indexOpts.map = indexOpts.map || {}
  if ('' != indexOpts.map[canonstr] && null != indexOpts.map[canonstr]) {
    return indexOpts.map[canonstr]
  }

  let prefix = indexOpts.prefix
  let suffix = indexOpts.suffix

  prefix = '' == prefix || null == prefix ? '' : prefix + '_'
  suffix = '' == suffix || null == suffix ? '' : '_' + suffix

  // TOOD: need ent.canon$({ external: true }) : foo/bar -> foo_bar
  let infix = ent
    .canon$({ string: true })
    .replace(/-\//g, '')
    .replace(/\//g, '_')

  return prefix + infix + suffix
}

// Default options.
const defaults: Options = {
  debug: false,
  map: Any(),
  index: {
    prefix: '',
    suffix: '',
    map: {},
    exact: '',
  },

  // '' === name => do not inject
  field: {
    zone: { name: 'zone' },
    base: { name: 'base' },
    name: { name: 'name' },
    vector: { name: 'vector' },
  },

  cmd: {
    list: {
      size: 11,
    },
  },

  aws: Open({
    region: 'us-east-1',
  }),

  weaviate: Open({
    node: 'NODE-URL',
  }),
}

Object.assign(WeaviateStore, {
  defaults,
  utils: { resolveIndex },
})

export default WeaviateStore

if ('undefined' !== typeof module) {
  module.exports = WeaviateStore
}
