/**
 * What a job's data looks like after the trip through the database. pg-boss
 * stores it as JSON, so a handler sees a `Date` as a string, values with a
 * `toJSON()` method as what that returns, and functions and symbols not at all
 * (or as `null` in an array). Plain JSON types come back unchanged, so
 * `Jsonified<Plain>` is the same type as `Plain`.
 */
export type Jsonified<Value> = 0 extends 1 & Value
  ? any
  : unknown extends Value
    ? Value
    : Value extends { toJSON(): infer Serialized }
      ? Jsonified<Serialized>
      : Value extends string | number | boolean | null | undefined
        ? Value
        : Value extends bigint
          ? never
          : Value extends readonly unknown[]
            ? { [Index in keyof Value]: JsonifiedElement<Value[Index]> }
            : Value extends object
              ? JsonifiedObject<Value>
              : never;

/** Values `JSON.stringify` leaves out of an object. */
type Dropped = symbol | ((...args: any[]) => unknown);

/** A value that always disappears, sometimes disappears, or always stays. */
type Presence<Value> = 0 extends 1 & Value
  ? "stays"
  : [Exclude<Value, Dropped>] extends [never]
    ? "gone"
    : [Extract<Value, Dropped>] extends [never]
      ? "stays"
      : "maybe";

type JsonifiedObject<Value> = {
  [
    Key in keyof Value as Key extends symbol
      ? never
      : Presence<Value[Key]> extends "stays"
        ? Key
        : never
  ]: Jsonified<Value[Key]>;
} & {
  [
    Key in keyof Value as Key extends symbol
      ? never
      : Presence<Value[Key]> extends "maybe"
        ? Key
        : never
  ]?: Jsonified<Exclude<Value[Key], Dropped>>;
};

/** Where an object drops a value JSON cannot hold, an array needs `null` instead. */
type JsonifiedElement<Value> = Value extends { toJSON(): infer Serialized }
  ? JsonifiedElement<Serialized>
  : Value extends undefined | Dropped
    ? null
    : Jsonified<Value>;
