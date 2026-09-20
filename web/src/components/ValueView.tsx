// Recursive type-aware renderer for the protobuf Value (RESP3 mirror,
// proto/ultima/v1/command.proto). blob_string decodes UTF-8 with a hex
// fallback for non-printable bytes; maps render as tables; arrays/sets as
// lists; errors in red; push frames inline.
import type { Value } from "../../../gen/ts/ultima/v1/command_pb";

const dec = new TextDecoder("utf-8", { fatal: true });

/** Decode blob bytes as UTF-8; fall back to a hex dump when the bytes are
 * not valid printable UTF-8. */
export function blobToString(b: Uint8Array): { text: string; hex: boolean } {
  try {
    const s = dec.decode(b);
    // eslint-disable-next-line no-control-regex
    if (/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(s)) return { text: hexDump(b), hex: true };
    return { text: s, hex: false };
  } catch {
    return { text: hexDump(b), hex: true };
  }
}

function hexDump(b: Uint8Array): string {
  let s = "";
  for (const byte of b) s += byte.toString(16).padStart(2, "0");
  return `0x${s}`;
}

/** Flatten a Value to a single-line string (for table cells / keys). */
export function valueToString(v: Value | undefined): string {
  if (!v) return "";
  const k = v.kind;
  switch (k.case) {
    case "null":
      return "(null)";
    case "simpleString":
    case "error":
    case "bigNumber":
      return k.value;
    case "int":
      return k.value.toString();
    case "double":
      return String(k.value);
    case "bool":
      return k.value ? "true" : "false";
    case "blobString":
      return blobToString(k.value).text;
    case "verbatim":
      return blobToString(k.value.payload).text;
    case "array":
    case "set":
    case "push":
      return k.value.elems.map((e) => valueToString(e)).join(" ");
    case "map":
      return k.value.pairs.map((p) => `${valueToString(p.key)}: ${valueToString(p.value)}`).join(" ");
    default:
      return "";
  }
}

export function ValueView({ value }: { value: Value }) {
  const k = value.kind;
  switch (k.case) {
    case undefined:
      return <span className="vv-null">(empty)</span>;
    case "null":
      return <span className="vv-null">(null)</span>;
    case "simpleString":
      return <span className="vv-simple">{k.value}</span>;
    case "error":
      return <span className="vv-error">{k.value}</span>;
    case "int":
      return <span className="vv-int">(integer) {k.value.toString()}</span>;
    case "double":
      return <span className="vv-double">(double) {k.value}</span>;
    case "bool":
      return <span className="vv-bool">(bool) {k.value ? "true" : "false"}</span>;
    case "bigNumber":
      return <span className="vv-int">(bignumber) {k.value}</span>;
    case "blobString": {
      const { text, hex } = blobToString(k.value);
      return <span className={hex ? "vv-blob vv-hex" : "vv-blob"}>{text === "" ? "(empty string)" : text}</span>;
    }
    case "verbatim":
      return (
        <span className="vv-blob">
          <span className="vv-tag">{k.value.format}</span> {blobToString(k.value.payload).text}
        </span>
      );
    case "array":
      return <ValueList elems={k.value.elems} />;
    case "set":
      return <ValueList elems={k.value.elems} set />;
    case "push":
      return (
        <div className="vv-push">
          <span className="vv-tag">push</span>
          <ValueList elems={k.value.elems} />
        </div>
      );
    case "map":
      return (
        <table className="vv-map">
          <tbody>
            {k.value.pairs.map((p, i) => (
              <tr key={i}>
                <td className="vv-map-key">{p.key ? <ValueView value={p.key} /> : null}</td>
                <td>{p.value ? <ValueView value={p.value} /> : null}</td>
              </tr>
            ))}
          </tbody>
        </table>
      );
    default:
      return <span className="vv-null">(unknown value)</span>;
  }
}

function ValueList({ elems, set = false }: { elems: Value[]; set?: boolean }) {
  if (elems.length === 0) return <span className="vv-null">(empty {set ? "set" : "array"})</span>;
  return (
    <ol className="vv-list">
      {elems.map((e, i) => (
        <li key={i}>
          <ValueView value={e} />
        </li>
      ))}
    </ol>
  );
}
