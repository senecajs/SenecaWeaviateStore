type Options = {
    debug: boolean;
    map?: any;
    field: {
        zone: {
            name: string;
        };
        base: {
            name: string;
        };
        name: {
            name: string;
        };
        vector: {
            name: string;
        };
    };
    url: string;
    client: any;
    collection: Record<string, any>;
    query: {
        limit: number;
    };
};
export type WeaviateStoreOptions = Partial<Options>;
declare function WeaviateStore(this: any, options: Options): {
    name: string;
    tag: any;
    exportmap: {
        native: () => {
            client: any;
        };
    };
};
export default WeaviateStore;
