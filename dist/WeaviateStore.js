"use strict";
/* Copyright (c) 2024 Seneca contributors, MIT License */
Object.defineProperty(exports, "__esModule", { value: true });
const { default: Weaviate } = require('weaviate-client');
const gubu_1 = require("gubu");
const { Child, Any } = gubu_1.Gubu;
function WeaviateStore(options) {
    const seneca = this;
    const init = seneca.export('entity/init');
    const colmap = {};
    let client;
    let onlyCol;
    let desc = 'WeaviateStore';
    Object.entries(options.collection).map((entry) => {
        entry[1].name = entry[1].name || entry[0];
        entry[1].key = entry[0];
    });
    let store = {
        name: 'WeaviateStore',
        save: function (msg, reply) {
            // const seneca = this
            const ent = msg.ent;
            const canon = ent.canon$({ object: true });
            const coldef = resolveColDef(ent, onlyCol, options);
            const col = getCollection(coldef, ent);
            const body = ent.data$(false);
            // TODO: move to entity as a util?
            const fieldOpts = options.field;
            ['zone', 'base', 'name'].forEach((n) => {
                if ('' != fieldOpts[n].name && null != canon[n] && '' != canon[n]) {
                    body[fieldOpts[n].name] = canon[n];
                }
            });
            const req = {
                properties: body
            };
            col
                .data
                .insert(req)
                .then((res) => {
                ent.data$(body);
                ent.id = res;
                reply(ent);
            })
                .catch((err) => {
                reply(null, err);
            });
        },
        load: function (msg, reply) {
            const ent = msg.ent;
            const coldef = resolveColDef(ent, onlyCol, options);
            const col = getCollection(coldef, ent);
            let q = msg.q || {};
            if (null != q.id) {
                col
                    .query
                    .fetchObjectById(q.id)
                    .then((res) => {
                    if (res) {
                        const body = res.properties;
                        ent.data$(body);
                        ent.id = q.id;
                        reply(ent);
                    }
                    else {
                        reply(null);
                    }
                })
                    .catch((err) => {
                    // Not found
                    if (err.meta && 404 === err.meta.statusCode) {
                        reply(null);
                    }
                    reply(null, err);
                });
            }
            else {
                reply(null);
            }
        },
        list: function (msg, reply) {
            // const seneca = this
            const ent = msg.ent;
            const q = msg.q;
            const coldef = resolveColDef(ent, onlyCol, options);
            const col = getCollection(coldef, ent);
            console.log('COL', col);
            const ntp = coldef.query.nearText.properties;
            const query = ntp
                .reduce((qs, pn) => (qs.push(q[pn]), qs), [])
                .filter((s) => null != s && '' !== s);
            console.log('LIST-A', ntp, query);
            const filters = Object
                .entries(q)
                .filter((n) => !n[0].match(/\$/))
                .reduce((a, n) => {
                console.log('F', n, a);
                if (ntp.includes(n[0])) {
                    return a;
                }
                let fx = col.filter.byProperty(n[0]).equal(n[1]);
                if (null != a) {
                    fx = a.and(fx);
                }
                return fx;
            }, null);
            const config = {
                filters,
                limit: options.query.limit,
                returnProperties: ['chunk', 'owner'],
                ...(q.config$ || {})
            };
            console.dir(coldef, { depth: null });
            console.log('LIST', query, config);
            col
                .query
                // .nearText(query, config)
                .nearText(query)
                .then((res) => {
                // console.log('RES', res)
                const hits = res.objects;
                const list = hits.map((entry) => {
                    let item = ent.make$().data$(entry.properties);
                    item.id = entry.uuid;
                    // item.custom$ = { score: entry._score }
                    return item;
                });
                reply(list);
            })
                .catch((err) => {
                // console.log('ERR', err)
                let ex = seneca.error('list$: ' + err.message, err);
                // console.log('EX', ex)
                reply(ex);
            });
        },
        // NOTE: all$:true is REQUIRED for deleteByQuery
        remove: function (msg, reply) {
            const ent = msg.ent;
            const coldef = resolveColDef(ent, onlyCol, options);
            const col = getCollection(coldef, ent);
            let q = msg.q || {};
            if (null != q.id) {
                col
                    .query
                    .deleteById(q.id)
                    .then((_res) => {
                    reply(null);
                })
                    .catch((err) => {
                    reply(null, err);
                });
            }
            else {
                reply(null);
            }
        },
        close: function (_msg, reply) {
            this.log.debug('close', desc);
            reply();
        },
        // TODO: obsolete - remove from seneca entity
        native: function (_msg, reply) {
            reply(null, {
                client: () => client,
            });
        },
    };
    let meta = init(seneca, options, store);
    desc = meta.desc;
    seneca.prepare(async function () {
        client = await Weaviate.connectToWCS(options.url, options.client);
        const coldefs = Object.values(options.collection);
        for (let cdI = 0; cdI < coldefs.length; cdI++) {
            const coldef = coldefs[cdI];
            coldef.config = coldef.config();
            console.log('COLDEF', cdI, coldef);
            let col;
            const allcols = await client.collections.listAll();
            const allcolnames = allcols.map((c) => c.name);
            if (allcolnames.includes(coldef.name)) {
                try {
                    col = await client.collections.get(coldef.name);
                    console.log('COL', coldef.name, col);
                }
                catch (e) {
                    seneca.log.warn('collection-get-failed', { name: coldef.name });
                }
            }
            else {
                seneca.log.warn('collection-not-exist', { name: coldef.name });
            }
            if (null == col) {
                const colspec = {
                    name: coldef.name,
                    ...coldef.config,
                };
                console.log('COLSPEC', coldef.name, colspec);
                col = await client.collections.create(colspec);
                console.log('COL CREATED', coldef.name, col);
            }
            colmap[coldef.key] = col;
            coldef.query = coldef.query || {};
            coldef.query.nearText = coldef.query.nearText || {};
            coldef.query.nearText.properties = coldef.query.nearText.properties || [];
            // If not specified, use first text prpperty
            let ntp = coldef.query.nearText.properties;
            if (0 === ntp.length) {
                for (let pI = 0; pI < coldef.config.properties.length; pI++) {
                    let pdef = coldef.config.properties[pI];
                    if ('text' === pdef.dataType) {
                        ntp.push(pdef.name);
                        break;
                    }
                }
            }
        }
        if (1 === coldefs.length) {
            onlyCol = coldefs[0];
        }
    });
    function getCollection(coldef, ent) {
        if (null === coldef) {
            seneca.fail('no-collection-defined', { ent });
        }
        // options.collection might use entity canon as key - not a valid weaviate collection name
        const key = coldef.key;
        const name = coldef.name;
        const col = colmap[key] || colmap[name];
        if (null === col) {
            seneca.fail('no-collection-found', { key, name, ent });
        }
        return col;
    }
    return {
        name: store.name,
        tag: meta.tag,
        exportmap: {
            native: () => {
                return { client };
            },
        },
    };
}
function resolveColDef(ent, onlyCol, options) {
    if (null != onlyCol) {
        return onlyCol;
    }
    let canonstr = ent.canon$({ string: true });
    let coldefmap = options.collection;
    if (null != coldefmap[canonstr]) {
        return coldefmap[canonstr];
    }
    return null;
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
    },
    url: String,
    client: {},
    collection: Child({}),
    query: {
        limit: 11,
    }
};
Object.assign(WeaviateStore, {
    defaults,
    utils: { resolveColDef },
});
exports.default = WeaviateStore;
if ('undefined' !== typeof module) {
    module.exports = WeaviateStore;
}
//# sourceMappingURL=WeaviateStore.js.map