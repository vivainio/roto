-- Luau definitions for service modules (`--lua-services` bundles): the `(ctx, input, db)` handler
-- signature, the `db` document store, and the `roto` helpers a module sees. Setup scripts use
-- `roto.d.luau` instead; the two files declare different `roto` tables and must not be mixed.

declare extern type RotoNull with end

-- Request context. Extern types are what a module can name in an annotation, so these are
-- `declare extern type`s rather than aliases.
declare extern type Ctx with
	account_id: string
	region: string
	request_id: string
end

-- Documents are plain JSON values chosen by the service; annotate the result of `get` and
-- `list` with the service's own document type. `get` returns nil when the document is absent. The functions are called with `.`, not `:`.
declare extern type Db with
	get: (namespace: string, id: string) -> any?
	put: (namespace: string, id: string, document: any) -> ()
	delete: (namespace: string, id: string) -> ()
	list: (namespace: string) -> { any }
end

declare roto: {
	version: string,
	api_version: number,
	null: RotoNull,
	-- Raises a modeled AWS client error and rolls the request back. `status` defaults to 400.
	error: (code: string, message: string, status: number?) -> never,
	id: (prefix: string) -> string,
	uuid: () -> string,
	sha256: (text: string) -> string,
	decode_base64: (text: string) -> string,
	json_decode: (text: string, code: string?) -> any,
	-- Marks a table as a JSON array, so an empty one encodes as `[]` rather than `{}`.
	array: (items: { any }?) -> { any },

	-- Helpers defined by the bundle's `common.luau`.
	required: (input: any, key: string, code: string?) -> string,
	object: (value: any, code: string?, label: string?) -> { [string]: any },
	list: (value: any, code: string?, label: string?) -> { any },
	scope: (ctx: Ctx, collection: string, global: boolean?) -> string,
	contains: (values: { any }?, target: any) -> boolean,
	page: (input: any, values: { any }, key: string, default: number?, cap: number?, code: string?) -> { [string]: any },
	tags: (existing: { { Key: string, Value: string } }?, incoming: any, code: string?) -> { { Key: string, Value: string } },
	untag: (existing: { { Key: string, Value: string } }?, keys: any, code: string?) -> { { Key: string, Value: string } },
	copy_fields: (source: any, destination: any, fields: { string }) -> any,
}
