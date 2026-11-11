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
	-- Timers: queue a call of the module's `__job(ctx, job, db)` function. `delay` is seconds from
	-- now, `at` is a Unix time in seconds (give at most one); `payload` is any JSON value. The job
	-- is written in the request's transaction. Scheduling an existing key replaces that job.
	schedule: (key: string, options: { delay: number?, at: number?, payload: any? }?) -> ()
	-- Removes the job with this key, if any.
	cancel: (key: string) -> ()
end

-- Result of `roto.try_call` and `roto.try_wire`: `ok` is true with the response, or false with
-- the callee's HTTP status and AWS error code and message.
type RotoPeerResult = {
	ok: boolean,
	value: any?,
	status: number?,
	code: string?,
	message: string?,
	headers: { [string]: string }?,
	body: string?,
}

declare roto: {
	version: string,
	api_version: number,
	null: RotoNull,
	-- Raises a modeled AWS client error and rolls the request back. `status` defaults to 400.
	error: (code: string, message: string, status: number?) -> never,
	error_info: (value: any) -> { code: string, message: string, status: number }?,
	id: (prefix: string) -> string,
	uuid: () -> string,
	-- Seconds since the Unix epoch on the virtual clock (fractional); `os.time()` uses it too.
	now: () -> number,
	sha256: (text: string) -> string,
	hash: (text: string, algorithm: "MD5" | "SHA-1" | "SHA-256" | "SHA-384" | "SHA-512") -> string,
	decode_base64: (text: string) -> string,
	encode_base64: (text: string) -> string,
	-- Compact JSON text of a value; whole numbers print as integers.
	json_encode: (value: any) -> string,
	json_decode: (text: string, code: string?) -> any,
	validate_mock_result: (service: string, operation: string, value: any, strict: boolean) -> string?,
	-- Marks a table as a JSON array, so an empty one encodes as `[]` rather than `{}`.
	array: (items: { any }?) -> { any },

	-- Calls another service in-process, as the caller's account and region, while a handler or job
	-- runs. The callee commits in its own transaction: there is no atomicity across services. A
	-- service cannot call itself or one already running on this call stack, and calls nest at most
	-- eight deep. `call` raises when the callee fails; `try_call` returns the failure as data.
	-- Typed protocols (JSON, query, rest-xml services with an AWS model) only; `wire` sends a raw request.
	call: (service: string, operation: string, input: any?) -> any,
	try_call: (service: string, operation: string, input: any?) -> RotoPeerResult,
	try_call_timed: (service: string, operation: string, input: any?, timeoutSeconds: number) -> RotoPeerResult,
	wire: (service: string, method: string, path: string, options: { headers: { [string]: string }?, query: string?, body: any? }?) -> RotoPeerResult,
	try_wire: (service: string, method: string, path: string, options: { headers: { [string]: string }?, query: string?, body: any? }?) -> RotoPeerResult,
	try_wire_timed: (service: string, method: string, path: string, options: { headers: { [string]: string }?, query: string?, body: any? }?, timeoutSeconds: number) -> RotoPeerResult,

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
