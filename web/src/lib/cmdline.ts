// Console input tokenizer: shellish whitespace split with single/double
// quotes and backslash escapes. Single quotes are literal (no escapes
// inside); double quotes honor \\ \" \n \t \r \xNN; a backslash outside
// quotes escapes the next character. Unterminated quotes are an error.

export class TokenizeError extends Error {}

export function tokenize(input: string): string[] {
  const out: string[] = [];
  let cur = "";
  let has = false; // cur holds a (possibly empty) token
  let i = 0;
  const n = input.length;

  const push = () => {
    out.push(cur);
    cur = "";
    has = false;
  };

  while (i < n) {
    const c = input[i] as string;
    if (c === " " || c === "\t" || c === "\n" || c === "\r") {
      if (has) push();
      i++;
      continue;
    }
    if (c === "\\") {
      if (i + 1 >= n) throw new TokenizeError("trailing backslash");
      cur += input[i + 1] as string;
      has = true;
      i += 2;
      continue;
    }
    if (c === "'") {
      i++;
      has = true;
      while (i < n && input[i] !== "'") {
        cur += input[i] as string;
        i++;
      }
      if (i >= n) throw new TokenizeError("unterminated single quote");
      i++;
      continue;
    }
    if (c === '"') {
      i++;
      has = true;
      while (i < n && input[i] !== '"') {
        const d = input[i] as string;
        if (d === "\\") {
          if (i + 1 >= n) throw new TokenizeError("trailing backslash in double quotes");
          const e = input[i + 1] as string;
          if (e === "n") cur += "\n";
          else if (e === "t") cur += "\t";
          else if (e === "r") cur += "\r";
          else if (e === "x" && i + 3 < n) {
            const hex = input.slice(i + 2, i + 4);
            if (!/^[0-9a-fA-F]{2}$/.test(hex)) throw new TokenizeError(`bad \\x escape: ${hex}`);
            cur += String.fromCharCode(parseInt(hex, 16));
            i += 2;
          } else cur += e;
          i += 2;
          continue;
        }
        cur += d;
        i++;
      }
      if (i >= n) throw new TokenizeError("unterminated double quote");
      i++;
      continue;
    }
    cur += c;
    has = true;
    i++;
  }
  if (has) push();
  return out;
}

/** Split a console line into command + args, or null for an empty line. */
export function parseLine(input: string): { command: string; args: string[] } | null {
  const parts = tokenize(input);
  const command = parts[0];
  if (command === undefined) return null;
  return { command, args: parts.slice(1) };
}
