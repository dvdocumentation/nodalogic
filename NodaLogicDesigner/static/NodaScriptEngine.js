/* =============================================================================
   NodaScriptEngine.js  (JS port of NodaScript.java)
   -----------------------------------------------------------------------------
   Implements: Lexer + Parser(Pratt) + Evaluator + strict Value types.
   EN + RU/1C keywords, case-insensitive identifiers/keywords.
   compile() caches AST for performance (#expr in huge lists).
   External functions: engine.register(name, fn) where fn(args, runtime) -> JS value.
   Builtins: Now/ParseDate/FormatDate/AddDays/AddMonths/AddYears/StartOfX/EndOfX,
             String/Length/Substring/IndexOf/HasProperty(+ alias Свойство),
             NewArray/NewObject/NewStructure (+ aliases).
   Array methods: arr.add/clear/contains.
   ============================================================================= */

class NodaScriptEngine {
  constructor(options = {}) {
    this.maxInstructions = options.maxInstructions ?? 1_000_000;
    this.tzOffsetMinutes = options.tzOffsetMinutes ?? null; // null => local TZ
    this._externals = new Map(); // nameNorm -> fn(argsJsArray, runtime) => jsValue
    this._cache = new Map();     // code -> program AST
  }

  register(name, fn) {
    if (!name || !String(name).trim()) throw new Error("Function name is blank");
    if (typeof fn !== "function") throw new Error("Fn must be a function");
    this._externals.set(String(name).toLowerCase(), fn);
    return this;
  }

  compile(code) {
    const src = String(code ?? "");
    const hit = this._cache.get(src);
    if (hit) return hit;
    const p = new Parser(src);
    const prog = p.parseProgram();
    this._cache.set(src, prog);
    return prog;
  }

  execute(codeOrProg, dataRoot, ctx = {}) {
    const prog = typeof codeOrProg === "string" ? this.compile(codeOrProg) : codeOrProg;
    const rt = new Runtime(Value.obj(dataRoot), {
      maxInstructions: this.maxInstructions,
      tzOffsetMinutes: this.tzOffsetMinutes,
      externals: this._externals,
      ctx
    });
    const ev = new Evaluator(rt);
    ev.execProgram(prog);
    return dataRoot; // mutated in-place
  }

  get(codeOrProg, dataRoot, ctx = {}) {
    const prog = typeof codeOrProg === "string" ? this.compile(codeOrProg) : codeOrProg;
    const rt = new Runtime(Value.obj(dataRoot), {
      maxInstructions: this.maxInstructions,
      tzOffsetMinutes: this.tzOffsetMinutes,
      externals: this._externals,
      ctx
    });
    const ev = new Evaluator(rt);
    const ret = ev.execProgramGet(prog);
    return ret.isNil() ? null : ret.toJS();
  }
}

/* ========================= Errors + SourcePos ========================= */

class SourcePos {
  constructor(line, col) { this.line = line; this.col = col; }
  static UNKNOWN = new SourcePos(-1, -1);
}
class ScriptError extends Error {
  constructor(kind, pos, message) {
    super(message);
    this.kind = kind; // parse/type/runtime
    this.pos = pos ?? SourcePos.UNKNOWN;
  }
  static parse(pos, msg) { return new ScriptError("parse", pos, msg); }
  static type(pos, msg) { return new ScriptError("type", pos, msg); }
  static runtime(pos, msg) { return new ScriptError("runtime", pos, msg); }
}

/* =============================== Value =============================== */

class Value {
  static NIL = 0;
  static BOOL = 1;
  static INT = 2;
  static FLOAT = 3;
  static STR = 4;
  static ARR = 5;
  static OBJ = 6;
  static DATE = 7;

  constructor(t, v) { this.t = t; this.v = v; }

  static nil() { return new Value(Value.NIL, null); }
  static bool(b) { return new Value(Value.BOOL, !!b); }
  static i(n) { return new Value(Value.INT, Number(n)); }
  static f(n) { return new Value(Value.FLOAT, Number(n)); }
  static str(s) { return new Value(Value.STR, String(s ?? "")); }
  static arr(a) { return new Value(Value.ARR, a); } // JS array
  static obj(o) { return new Value(Value.OBJ, o); } // JS object
  static date(ms) { return new Value(Value.DATE, Number(ms)); }

  isNil() { return this.t === Value.NIL; }

  asBool(pos) { if (this.t !== Value.BOOL) throw ScriptError.type(pos, "Expected boolean"); return !!this.v; }
  asInt(pos) { if (this.t !== Value.INT) throw ScriptError.type(pos, "Expected int"); return this.v; }
  asNum(pos) {
    if (this.t === Value.INT || this.t === Value.FLOAT) return this.v;
    throw ScriptError.type(pos, "Expected number");
  }
  asStr(pos) { if (this.t !== Value.STR) throw ScriptError.type(pos, "Expected string"); return this.v; }
  asArr(pos) { if (this.t !== Value.ARR) throw ScriptError.type(pos, "Expected array"); return this.v; }
  asObj(pos) { if (this.t !== Value.OBJ) throw ScriptError.type(pos, "Expected object"); return this.v; }
  asDate(pos) { if (this.t !== Value.DATE) throw ScriptError.type(pos, "Expected date"); return this.v; }

  length(pos) {
    if (this.t === Value.STR) return Value.i(this.v.length);
    if (this.t === Value.ARR) return Value.i(this.v.length);
    if (this.t === Value.OBJ) return Value.i(Object.keys(this.v ?? {}).length);
    throw ScriptError.type(pos, "Length expects string/array/object");
  }

  toJS() {
    switch (this.t) {
      case Value.NIL: return null;
      case Value.BOOL: return this.v;
      case Value.INT: return this.v;
      case Value.FLOAT: return this.v;
      case Value.STR: return this.v;
      case Value.DATE: return this.v;
      case Value.ARR: return this.v;
      case Value.OBJ: return this.v;
      default: return null;
    }
  }

  static fromJS(x) {
    if (x === null || x === undefined) return Value.nil();
    if (x instanceof Value) return x;
    if (typeof x === "boolean") return Value.bool(x);
    if (typeof x === "number") {
      if (Number.isFinite(x) && Math.floor(x) === x) return Value.i(x);
      return Value.f(x);
    }
    if (typeof x === "string") return Value.str(x);
    if (Array.isArray(x)) return Value.arr(x);
    if (typeof x === "object") return Value.obj(x);
    return Value.str(String(x));
  }

  static isNum(v) { return v.t === Value.INT || v.t === Value.FLOAT; }

  static add(a, b, pos) {
    if (a.t === Value.INT && b.t === Value.INT) return Value.i(a.v + b.v);
    if (Value.isNum(a) && Value.isNum(b)) return Value.f(a.v + b.v);
    if (a.t === Value.STR && b.t === Value.STR) return Value.str(a.v + b.v);
    throw ScriptError.type(pos, "Operator + expects (number,number) or (string,string)");
  }
  static sub(a, b, pos) {
    if (a.t === Value.INT && b.t === Value.INT) return Value.i(a.v - b.v);
    if (Value.isNum(a) && Value.isNum(b)) return Value.f(a.v - b.v);
    throw ScriptError.type(pos, "Operator - expects (number,number)");
  }
  static mul(a, b, pos) {
    if (a.t === Value.INT && b.t === Value.INT) return Value.i(a.v * b.v);
    if (Value.isNum(a) && Value.isNum(b)) return Value.f(a.v * b.v);
    throw ScriptError.type(pos, "Operator * expects (number,number)");
  }
  static div(a, b, pos) {
    if (!Value.isNum(a) || !Value.isNum(b)) throw ScriptError.type(pos, "Operator / expects (number,number)");
    if (b.v === 0) throw ScriptError.runtime(pos, "Division by zero");
    return Value.f(a.v / b.v);
  }
  static neg(x, pos) {
    if (x.t === Value.INT) return Value.i(-x.v);
    if (x.t === Value.FLOAT) return Value.f(-x.v);
    throw ScriptError.type(pos, "Unary - expects number");
  }
  static not(x, pos) {
    if (x.t !== Value.BOOL) throw ScriptError.type(pos, "NOT expects boolean");
    return Value.bool(!x.v);
  }
  static and(a, b, pos) {
    if (a.t !== Value.BOOL || b.t !== Value.BOOL) throw ScriptError.type(pos, "AND expects boolean");
    return Value.bool(a.v && b.v);
  }
  static or(a, b, pos) {
    if (a.t !== Value.BOOL || b.t !== Value.BOOL) throw ScriptError.type(pos, "OR expects boolean");
    return Value.bool(a.v || b.v);
  }
  static eq(a, b) {
    if (Value.isNum(a) && Value.isNum(b)) return Value.bool(a.v === b.v);
    if (a.t !== b.t) return Value.bool(false);
    if (a.t === Value.NIL) return Value.bool(true);
    if (a.t === Value.BOOL) return Value.bool(a.v === b.v);
    if (a.t === Value.INT || a.t === Value.FLOAT) return Value.bool(a.v === b.v);
    if (a.t === Value.STR) return Value.bool(a.v === b.v);
    if (a.t === Value.DATE) return Value.bool(a.v === b.v);
    return Value.bool(a.v === b.v); // identity for arrays/objects
  }
  static ne(a, b) { return Value.bool(!Value.eq(a, b).v); }

  static cmp(a, b, op, pos) {
    if (Value.isNum(a) && Value.isNum(b)) {
      const x = a.v, y = b.v;
      if (op === ">") return Value.bool(x > y);
      if (op === ">=") return Value.bool(x >= y);
      if (op === "<") return Value.bool(x < y);
      if (op === "<=") return Value.bool(x <= y);
    }
    if (a.t === Value.STR && b.t === Value.STR) {
      const c = a.v.localeCompare(b.v);
      if (op === ">") return Value.bool(c > 0);
      if (op === ">=") return Value.bool(c >= 0);
      if (op === "<") return Value.bool(c < 0);
      if (op === "<=") return Value.bool(c <= 0);
    }
    if (a.t === Value.DATE && b.t === Value.DATE) {
      const x = a.v, y = b.v;
      if (op === ">") return Value.bool(x > y);
      if (op === ">=") return Value.bool(x >= y);
      if (op === "<") return Value.bool(x < y);
      if (op === "<=") return Value.bool(x <= y);
    }
    throw ScriptError.type(pos, "Comparison not supported for given types");
  }

  static inOp(left, right, pos) {
    if (right.t === Value.ARR) {
      const arr = right.v;
      for (let i = 0; i < arr.length; i++) {
        if (Value.eq(left, Value.fromJS(arr[i])).v) return Value.bool(true);
      }
      return Value.bool(false);
    }
    if (right.t === Value.OBJ) {
      if (left.t !== Value.STR) throw ScriptError.type(pos, "IN with object expects string key on left");
      return Value.bool(Object.prototype.hasOwnProperty.call(right.v, left.v));
    }
    throw ScriptError.type(pos, "IN expects array or object on right");
  }
}

/* =============================== Lexer =============================== */

const TokType = Object.freeze({
  IDENT:"IDENT", INT:"INT", FLOAT:"FLOAT", STR:"STR",
  IF:"IF", ELSE:"ELSE", WHILE:"WHILE",
  FOR:"FOR", IN:"IN",
  TRY:"TRY", EXCEPT:"EXCEPT", THROW:"THROW",
  BREAK:"BREAK", CONTINUE:"CONTINUE",
  TRUE:"TRUE", FALSE:"FALSE", NULL:"NULL",
  AND:"AND", OR:"OR", NOT:"NOT",
  RETURN:"RETURN",

  ASSIGN:"ASSIGN",
  PLUS:"PLUS", MINUS:"MINUS", STAR:"STAR", SLASH:"SLASH",
  EQEQ:"EQEQ", NOTEQ:"NOTEQ",
  GT:"GT", LT:"LT", GTE:"GTE", LTE:"LTE",
  ANDAND:"ANDAND", OROR:"OROR", BANG:"BANG",
  QUESTION:"QUESTION", COLON:"COLON",
  DOT:"DOT", COMMA:"COMMA", SEMI:"SEMI",
  LPAREN:"LPAREN", RPAREN:"RPAREN", LBRACE:"LBRACE", RBRACE:"RBRACE", LBRACK:"LBRACK", RBRACK:"RBRACK",

  THEN:"THEN", ELSEIF:"ELSEIF", ENDIF:"ENDIF", CYCLE:"CYCLE", ENDCYCLE:"ENDCYCLE",
  EACH:"EACH", TO:"TO", NEW:"NEW", ARRAYKW:"ARRAYKW", STRUCTKW:"STRUCTKW",

  EOF:"EOF"
});

class Tok {
  constructor(t, lexeme, lit, pos) { this.t=t; this.lexeme=lexeme; this.lit=lit; this.pos=pos; }
}

class Lexer {
  constructor(s) {
    this.s = String(s ?? "");
    this.i = 0;
    this.line = 1;
    this.col = 1;
  }

  static KW = (() => {
    const m = new Map();
    // EN
    m.set("if", TokType.IF);
    m.set("else", TokType.ELSE);
    m.set("while", TokType.WHILE);
    m.set("for", TokType.FOR);
    m.set("in", TokType.IN);
    m.set("try", TokType.TRY);
    m.set("except", TokType.EXCEPT);
    m.set("throw", TokType.THROW);
    m.set("break", TokType.BREAK);
    m.set("continue", TokType.CONTINUE);
    m.set("true", TokType.TRUE);
    m.set("false", TokType.FALSE);
    m.set("null", TokType.NULL);
    m.set("and", TokType.AND);
    m.set("or", TokType.OR);
    m.set("not", TokType.NOT);
    m.set("return", TokType.RETURN);

    // RU / 1C
    m.set("возврат", TokType.RETURN);
    m.set("если", TokType.IF);
    m.set("иначе", TokType.ELSE);
    m.set("пока", TokType.WHILE);

    m.set("для", TokType.FOR);
    m.set("в", TokType.IN);
    m.set("из", TokType.IN);

    m.set("попытка", TokType.TRY);
    m.set("исключение", TokType.EXCEPT);
    m.set("вызвать", TokType.THROW);

    m.set("прервать", TokType.BREAK);
    m.set("продолжить", TokType.CONTINUE);

    m.set("истина", TokType.TRUE);
    m.set("ложь", TokType.FALSE);
    m.set("пусто", TokType.NULL);
    m.set("неопределено", TokType.NULL);

    m.set("и", TokType.AND);
    m.set("или", TokType.OR);
    m.set("не", TokType.NOT);

    m.set("тогда", TokType.THEN);
    m.set("иначеесли", TokType.ELSEIF);
    m.set("конецесли", TokType.ENDIF);
    m.set("цикл", TokType.CYCLE);
    m.set("конеццикла", TokType.ENDCYCLE);
    m.set("каждого", TokType.EACH);
    m.set("по", TokType.TO);
    m.set("новый", TokType.NEW);
    m.set("массив", TokType.ARRAYKW);
    m.set("структура", TokType.STRUCTKW);
    return m;
  })();

  next() {
    this._skipWsAndComments();
    if (this._atEnd()) return new Tok(TokType.EOF, "", null, this._pos());

    const start = this._pos();
    const c = this._adv();

    switch (c) {
      case ';': return new Tok(TokType.SEMI, ";", null, start);
      case ',': return new Tok(TokType.COMMA, ",", null, start);
      case '.': return new Tok(TokType.DOT, ".", null, start);
      case '(': return new Tok(TokType.LPAREN, "(", null, start);
      case ')': return new Tok(TokType.RPAREN, ")", null, start);
      case '{': return new Tok(TokType.LBRACE, "{", null, start);
      case '}': return new Tok(TokType.RBRACE, "}", null, start);
      case '[': return new Tok(TokType.LBRACK, "[", null, start);
      case ']': return new Tok(TokType.RBRACK, "]", null, start);
      case '+': return new Tok(TokType.PLUS, "+", null, start);
      case '-': return new Tok(TokType.MINUS, "-", null, start);
      case '*': return new Tok(TokType.STAR, "*", null, start);
      case '?': return new Tok(TokType.QUESTION, "?", null, start);
      case ':': return new Tok(TokType.COLON, ":", null, start);
      case '/': return new Tok(TokType.SLASH, "/", null, start);
    }

    if (c === '!') {
      if (this._match('=')) return new Tok(TokType.NOTEQ, "!=", null, start);
      return new Tok(TokType.BANG, "!", null, start);
    }
    if (c === '=') {
      if (this._match('=')) return new Tok(TokType.EQEQ, "==", null, start);
      return new Tok(TokType.ASSIGN, "=", null, start);
    }
    if (c === '>') {
      if (this._match('=')) return new Tok(TokType.GTE, ">=", null, start);
      return new Tok(TokType.GT, ">", null, start);
    }
    if (c === '<') {
      if (this._match('=')) return new Tok(TokType.LTE, "<=", null, start);
      return new Tok(TokType.LT, "<", null, start);
    }
    if (c === '&') {
      if (this._match('&')) return new Tok(TokType.ANDAND, "&&", null, start);
      throw ScriptError.parse(start, "Unexpected '&' (did you mean '&&'?)");
    }
    if (c === '|') {
      if (this._match('|')) return new Tok(TokType.OROR, "||", null, start);
      throw ScriptError.parse(start, "Unexpected '|' (did you mean '||'?)");
    }

    if (c === '"' || c === "'") return this._readString(start, c);
    if (Lexer._isDigit(c)) return this._readNumber(start, c);
    if (Lexer._isIdentStart(c)) return this._readIdentOrKw(start, c);

    throw ScriptError.parse(start, `Unexpected character: '${c}'`);
  }

  _readString(start, quote) {
    let out = "";
    while (!this._atEnd()) {
      const c = this._adv();
      if (c === quote) return new Tok(TokType.STR, quote + out + quote, out, start);
      if (c === '\\') {
        if (this._atEnd()) throw ScriptError.parse(start, "Unterminated string escape");
        const e = this._adv();
        if (e === 'n') out += '\n';
        else if (e === 'r') out += '\r';
        else if (e === 't') out += '\t';
        else if (e === '"') out += '"';
        else if (e === "'") out += "'";
        else if (e === '\\') out += '\\';
        else throw ScriptError.parse(this._pos(), "Unknown escape: \\" + e);
        continue;
      }
      if (c === '\n') throw ScriptError.parse(start, "Newline in string literal");
      out += c;
    }
    throw ScriptError.parse(start, "Unterminated string literal");
  }

  _readNumber(start, first) {
    let text = first;
    while (Lexer._isDigit(this._peek())) text += this._adv();
    let isFloat = false;
    if (this._peek() === '.' && Lexer._isDigit(this._peekNext())) {
      isFloat = true;
      text += this._adv();
      while (Lexer._isDigit(this._peek())) text += this._adv();
    }
    const v = Number(text);
    if (!Number.isFinite(v)) throw ScriptError.parse(start, "Invalid number: " + text);
    return new Tok(isFloat ? TokType.FLOAT : TokType.INT, text, v, start);
  }

  _readIdentOrKw(start, first) {
    let lex = first;
    while (Lexer._isIdentPart(this._peek())) lex += this._adv();
    const lower = lex.toLowerCase();
    const kw = Lexer.KW.get(lower);
    if (kw) {
      let lit = null;
      if (kw === TokType.TRUE) lit = true;
      else if (kw === TokType.FALSE) lit = false;
      return new Tok(kw, lex, lit, start);
    }
    return new Tok(TokType.IDENT, lex, lower, start);
  }

  _skipWsAndComments() {
    while (!this._atEnd()) {
      const c = this._peek();
      if (c === ' ' || c === '\t' || c === '\r' || c === '\n') { this._adv(); continue; }
      if (c === '/' && this._peekNext() === '/') {
        while (!this._atEnd() && this._peek() !== '\n') this._adv();
        continue;
      }
      break;
    }
  }

  _match(ch) {
    if (this._atEnd()) return false;
    if (this.s.charAt(this.i) !== ch) return false;
    this._adv();
    return true;
  }

  _adv() {
    const c = this.s.charAt(this.i++);
    if (c === '\n') { this.line++; this.col = 1; }
    else this.col++;
    return c;
  }

  _atEnd() { return this.i >= this.s.length; }
  _peek() { return this._atEnd() ? '\0' : this.s.charAt(this.i); }
  _peekNext() { return (this.i+1 >= this.s.length) ? '\0' : this.s.charAt(this.i+1); }
  _pos() { return new SourcePos(this.line, this.col); }

  static _isDigit(c) { return c >= '0' && c <= '9'; }
  static _isIdentStart(c) { return c === '_' || (c && c !== '\0' && /\p{L}/u.test(c)); }
  static _isIdentPart(c) { return Lexer._isIdentStart(c) || Lexer._isDigit(c); }
}

/* =============================== Parser =============================== */

class Parser {
  constructor(code) {
    this.lx = new Lexer(code);
    this.cur = this.lx.next();
  }

  parseProgram() {
    const out = [];
    while (this.cur.t !== TokType.EOF) out.push(this._parseStatement());
    return out;
  }

  _parseStatement() {
    if (this.cur.t === TokType.SEMI) { const p=this.cur.pos; this._adv(); return {kind:"Empty", pos:p}; }

    if (this.cur.t === TokType.LBRACE) return this._parseBlock();
    if (this.cur.t === TokType.IF) return this._parseIf();
    if (this.cur.t === TokType.WHILE) return this._parseWhile();
    if (this.cur.t === TokType.FOR) return this._parseFor();
    if (this.cur.t === TokType.TRY) return this._parseTryExcept();
    if (this.cur.t === TokType.THROW) return this._parseThrow();
    if (this.cur.t === TokType.RETURN) return this._parseReturn();
    if (this.cur.t === TokType.BREAK) { const p=this.cur.pos; this._adv(); this._consumeOptionalSemicolon(); return {kind:"Break", pos:p}; }
    if (this.cur.t === TokType.CONTINUE) { const p=this.cur.pos; this._adv(); this._consumeOptionalSemicolon(); return {kind:"Continue", pos:p}; }

    const left = this._parseExpr(0);
    if (this.cur.t === TokType.ASSIGN) {
      const ap = this.cur.pos;
      const lv = this._asLValue(left);
      if (!lv) throw ScriptError.parse(ap, "Left side of assignment is not assignable");
      this._adv();
      const rhs = this._parseExpr(0);
      this._consumeSemicolonRequiredUnlessBlockEnd();
      return {kind:"Assign", target:lv, value:rhs, pos:ap};
    }
    this._consumeSemicolonRequiredUnlessBlockEnd();
    return {kind:"ExprStmt", expr:left, pos:left.pos};
  }

  _consumeOptionalSemicolon() { if (this.cur.t === TokType.SEMI) this._adv(); }
  _consumeSemicolonRequiredUnlessBlockEnd() {
    if (this.cur.t === TokType.SEMI) { this._adv(); return; }
    if (this.cur.t === TokType.RBRACE || this.cur.t === TokType.EOF ||
        this.cur.t === TokType.ELSE || this.cur.t === TokType.ELSEIF ||
        this.cur.t === TokType.ENDIF || this.cur.t === TokType.ENDCYCLE ||
        this.cur.t === TokType.EXCEPT) return;
    throw ScriptError.parse(this.cur.pos, "Expected ';'");
  }

  _parseBlock() {
    const p=this.cur.pos;
    this._expect(TokType.LBRACE, "Expected '{'"); this._adv();
    const stmts=[];
    while (this.cur.t !== TokType.RBRACE) {
      if (this.cur.t === TokType.EOF) throw ScriptError.parse(this.cur.pos, "Unterminated block");
      stmts.push(this._parseStatement());
    }
    this._adv();
    return {kind:"Block", stmts, pos:p};
  }

  _parseBlockUntil(stopTokens) {
    const p=this.cur.pos;
    const stmts=[];
    while (!stopTokens.includes(this.cur.t) && this.cur.t !== TokType.EOF) {
      stmts.push(this._parseStatement());
    }
    return {kind:"Block", stmts, pos:p};
  }

   _parseIf() {
    const p=this.cur.pos;
    this._adv();
    const cond=this._parseCondExpr(0);

    if (this.cur.t === TokType.THEN) {
      this._adv();
      const thenB = this._parseBlockUntil([TokType.ELSEIF, TokType.ELSE, TokType.ENDIF]);

      let elseB = null;
      const elifConds = [];
      const elifBlocks = [];

      while (this.cur.t === TokType.ELSEIF) {
        this._adv();
        const ec=this._parseCondExpr(0);
        this._expect(TokType.THEN, "Expected 'Тогда' after 'ИначеЕсли' condition"); this._adv();
        const eb=this._parseBlockUntil([TokType.ELSEIF, TokType.ELSE, TokType.ENDIF]);
        elifConds.push(ec);
        elifBlocks.push(eb);
      }

      if (this.cur.t === TokType.ELSE) {
        this._adv();
        elseB = this._parseBlockUntil([TokType.ENDIF]);
      }

      for (let i = elifConds.length - 1; i >= 0; i--) {
        const ec = elifConds[i];
        const eb = elifBlocks[i];
        const nested = {kind:"If", cond:ec, thenB:eb, elseB:elseB, pos:ec.pos};
        elseB = {kind:"Block", stmts:[nested], pos:ec.pos};
      }

      this._expect(TokType.ENDIF, "Expected 'КонецЕсли'"); this._adv();
      this._consumeOptionalSemicolon();
      return {kind:"If", cond, thenB, elseB, pos:p};
    }

    const thenB=this._parseBlock();
    let elseB=null;
    if (this.cur.t === TokType.ELSE) { this._adv(); elseB=this._parseBlock(); }
    return {kind:"If", cond, thenB, elseB, pos:p};
  }

    _parseWhile() {
    const p=this.cur.pos;
    this._adv();
    const cond=this._parseCondExpr(0);

    if (this.cur.t === TokType.CYCLE) {
      this._adv();
      const body=this._parseBlockUntil([TokType.ENDCYCLE]);
      this._expect(TokType.ENDCYCLE, "Expected 'КонецЦикла'"); this._adv();
      this._consumeOptionalSemicolon();
      return {kind:"While", cond, body, pos:p};
    }

    const body=this._parseBlock();
    return {kind:"While", cond, body, pos:p};
  }

  _parseFor() {
    const p=this.cur.pos;
    this._adv();

    // 1C foreach
    if (this.cur.t === TokType.EACH) {
      this._adv();
      this._expect(TokType.IDENT, "Expected variable after 'Каждого'");
      const varName=this.cur.lexeme; this._adv();
      this._expect(TokType.IN, "Expected 'Из' after variable"); this._adv();
      const iter=this._parseExpr(0);
      this._expect(TokType.CYCLE, "Expected 'Цикл'"); this._adv();
      const body=this._parseBlockUntil([TokType.ENDCYCLE]);
      this._expect(TokType.ENDCYCLE, "Expected 'КонецЦикла'"); this._adv();
      this._consumeOptionalSemicolon();
      return {kind:"ForIn", varName, iter, body, pos:p};
    }

    if (this.cur.t === TokType.IDENT) {
      const id=this.cur; this._adv();

      if (this.cur.t === TokType.ASSIGN) {
        this._adv();
        const start=this._parseExpr(0);

        if (this.cur.t === TokType.TO) {
          this._adv();
          const end=this._parseExpr(0);
          this._expect(TokType.CYCLE, "Expected 'Цикл'"); this._adv();
          const body=this._parseBlockUntil([TokType.ENDCYCLE]);
          this._expect(TokType.ENDCYCLE, "Expected 'КонецЦикла'"); this._adv();
          this._consumeOptionalSemicolon();
          return {kind:"ForRange", varName:id.lexeme, start, end, body, pos:p};
        }

        // c-style with init assignment
        const init = {kind:"Assign", target:{kind:"LVar", nameNorm:String(id.lit), pos:id.pos}, value:start, pos:id.pos};
        this._expect(TokType.SEMI, "Expected ';' after for-init"); this._adv();
        const cond=this._parseExpr(0);
        this._expect(TokType.SEMI, "Expected ';' after for-condition"); this._adv();
        const upLeft=this._parseExpr(0);

        let update;
        if (this.cur.t === TokType.ASSIGN) {
          const ap=this.cur.pos;
          const lv=this._asLValue(upLeft);
          if (!lv) throw ScriptError.parse(ap, "Left side of assignment is not assignable");
          this._adv();
          const rhs=this._parseExpr(0);
          update={kind:"Assign", target:lv, value:rhs, pos:ap};
        } else {
          update={kind:"ExprStmt", expr:upLeft, pos:upLeft.pos};
        }
        const body=this._parseBlock();
        return {kind:"ForC", init, cond, update, body, pos:p};
      }

      if (this.cur.t === TokType.IN) {
        this._adv();
        const iter=this._parseExpr(0);
        const body=this._parseBlock();
        return {kind:"ForIn", varName:id.lexeme, iter, body, pos:p};
      }

      throw ScriptError.parse(this.cur.pos, "Invalid for syntax");
    }

    // generic c-style
    const initLeft=this._parseExpr(0);
    let init;
    if (this.cur.t === TokType.ASSIGN) {
      const ap=this.cur.pos;
      const lv=this._asLValue(initLeft);
      if (!lv) throw ScriptError.parse(ap, "Left side of assignment is not assignable");
      this._adv();
      const rhs=this._parseExpr(0);
      init={kind:"Assign", target:lv, value:rhs, pos:ap};
    } else {
      init={kind:"ExprStmt", expr:initLeft, pos:initLeft.pos};
    }
    this._expect(TokType.SEMI, "Expected ';' after for-init"); this._adv();
    const cond=this._parseExpr(0);
    this._expect(TokType.SEMI, "Expected ';' after for-condition"); this._adv();

    const upLeft=this._parseExpr(0);
    let update;
    if (this.cur.t === TokType.ASSIGN) {
      const ap=this.cur.pos;
      const lv=this._asLValue(upLeft);
      if (!lv) throw ScriptError.parse(ap, "Left side of assignment is not assignable");
      this._adv();
      const rhs=this._parseExpr(0);
      update={kind:"Assign", target:lv, value:rhs, pos:ap};
    } else {
      update={kind:"ExprStmt", expr:upLeft, pos:upLeft.pos};
    }
    const body=this._parseBlock();
    return {kind:"ForC", init, cond, update, body, pos:p};
  }

  _parseTryExcept() {
    const p=this.cur.pos;
    this._adv();
    const tryB=this._parseBlock();
    this._expect(TokType.EXCEPT, "Expected 'except' after try block"); this._adv();
    this._expect(TokType.IDENT, "Expected identifier after except");
    const name=this.cur; this._adv();
    const exceptB=this._parseBlock();
    return {kind:"TryExcept", tryB, errNorm:String(name.lit), exceptB, pos:p};
  }

  _parseThrow() {
    const p=this.cur.pos;
    this._adv();
    const v=this._parseExpr(0);
    this._expect(TokType.SEMI, "Expected ';' after throw"); this._adv();
    return {kind:"Throw", value:v, pos:p};
  }

  _parseReturn() {
    const p=this.cur.pos;
    this._adv();
    if (this.cur.t === TokType.SEMI) { this._adv(); return {kind:"Return", value:null, pos:p}; }
    if (this.cur.t === TokType.RBRACE || this.cur.t === TokType.EOF ||
        this.cur.t === TokType.ELSE || this.cur.t === TokType.ELSEIF ||
        this.cur.t === TokType.ENDIF || this.cur.t === TokType.ENDCYCLE ||
        this.cur.t === TokType.EXCEPT) {
      return {kind:"Return", value:null, pos:p};
    }
    const v=this._parseExpr(0);
    this._consumeSemicolonRequiredUnlessBlockEnd();
    return {kind:"Return", value:v, pos:p};
  }

  // Pratt
    _parseCondExpr(minBp) {
    let left=this._parseCondPrefix();

    while (true) {
      if (this.cur.t === TokType.DOT) {
        const lbp=80;
        if (lbp < minBp) break;
        const p=this.cur.pos;
        this._adv();
        this._expect(TokType.IDENT, "Expected name after '.'");
        const nameTok=this.cur; this._adv();
        const name=nameTok.lexeme;

        if (this.cur.t === TokType.LPAREN) {
          this._adv();
          const args=[];
          if (this.cur.t !== TokType.RPAREN) {
            while (true) {
              args.push(this._parseCondExpr(0));
              if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
              break;
            }
          }
          this._expect(TokType.RPAREN, "Expected ')' after method args");
          this._adv();
          left={kind:"MethodCall", base:left, method:name, args, pos:p};
        } else {
          left={kind:"Prop", base:left, key:name, pos:p};
        }
        continue;
      }

      if (this.cur.t === TokType.LBRACK) {
        const lbp=80;
        if (lbp < minBp) break;
        const p=this.cur.pos;
        this._adv();
        const idx=this._parseCondExpr(0);
        this._expect(TokType.RBRACK, "Expected ']'");
        this._adv();
        left={kind:"Index", base:left, idx, pos:p};
        continue;
      }

      if (this.cur.t === TokType.QUESTION) {
        const lbp=10;
        if (lbp < minBp) break;
        const p=this.cur.pos;
        this._adv();
        const thenE=this._parseCondExpr(0);
        this._expect(TokType.COLON, "Expected ':' in ternary"); this._adv();
        const elseE=this._parseCondExpr(0);
        left={kind:"Ternary", c:left, t:thenE, f:elseE, pos:p};
        continue;
      }

      const op=this._infixOpCond(this.cur.t);
      if (!op) break;

      const lbp=this._infixLbp(op);
      const rbp=lbp + 1;
      if (lbp < minBp) break;

      const p=this.cur.pos;
      this._adv();
      const right=this._parseCondExpr(rbp);
      left={kind:"Binary", l:left, op, r:right, pos:p};
    }

    return left;
  }

  _parseCondPrefix() {
    const p=this.cur.pos;

    if (this.cur.t === TokType.QUESTION) {
      const qp=this.cur.pos;
      this._adv();
      this._expect(TokType.LPAREN, "Expected '(' after '?'");
      this._adv();
      const c=this._parseCondExpr(0);
      this._expect(TokType.COMMA, "Expected ',' after condition in ?(...)"); this._adv();
      const t=this._parseCondExpr(0);
      this._expect(TokType.COMMA, "Expected ',' after true-value in ?(...)"); this._adv();
      const f=this._parseCondExpr(0);
      this._expect(TokType.RPAREN, "Expected ')' after ?(...)"); this._adv();
      return {kind:"Ternary", c, t, f, pos:qp};
    }

    if (this.cur.t === TokType.NEW) {
      const np=this.cur.pos;
      this._adv();
      if (this.cur.t === TokType.ARRAYKW) {
        this._adv();
        return {kind:"ArrayLit", items:[], pos:np};
      }
      if (this.cur.t === TokType.STRUCTKW) {
        this._adv();
        this._expect(TokType.LPAREN, "Expected '(' after 'Новый Структура'");
        this._adv();
        const args=[];
        if (this.cur.t !== TokType.RPAREN) {
          while (true) {
            args.push(this._parseCondExpr(0));
            if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
            break;
          }
        }
        this._expect(TokType.RPAREN, "Expected ')' after Новый Структура(...)");
        this._adv();
        return {kind:"Call", fnNorm:"newstructure", args, pos:np};
      }
      throw ScriptError.parse(np, "Expected 'Массив' or 'Структура' after 'Новый'");
    }

    if (this.cur.t === TokType.MINUS) { this._adv(); return {kind:"Unary", op:"-", r:this._parseCondExpr(70), pos:p}; }
    if (this.cur.t === TokType.BANG) { this._adv(); return {kind:"Unary", op:"!", r:this._parseCondExpr(70), pos:p}; }
    if (this.cur.t === TokType.NOT) { this._adv(); return {kind:"Unary", op:"not", r:this._parseCondExpr(70), pos:p}; }

    if (this.cur.t === TokType.INT) { const v=this.cur.lit; this._adv(); return {kind:"Lit", v:Value.i(v), pos:p}; }
    if (this.cur.t === TokType.FLOAT) { const v=this.cur.lit; this._adv(); return {kind:"Lit", v:Value.f(v), pos:p}; }
    if (this.cur.t === TokType.STR) { const v=this.cur.lit; this._adv(); return {kind:"Lit", v:Value.str(v), pos:p}; }
    if (this.cur.t === TokType.TRUE) { this._adv(); return {kind:"Lit", v:Value.bool(true), pos:p}; }
    if (this.cur.t === TokType.FALSE) { this._adv(); return {kind:"Lit", v:Value.bool(false), pos:p}; }
    if (this.cur.t === TokType.NULL) { this._adv(); return {kind:"Lit", v:Value.nil(), pos:p}; }

    if (this.cur.t === TokType.LPAREN) {
      this._adv();
      const e=this._parseCondExpr(0);
      this._expect(TokType.RPAREN, "Expected ')'");
      this._adv();
      return e;
    }

    if (this.cur.t === TokType.LBRACE) {
      this._adv();
      const entries=[];
      if (this.cur.t !== TokType.RBRACE) {
        while (true) {
          let key;
          if (this.cur.t === TokType.IDENT) { key=this.cur.lexeme; this._adv(); }
          else if (this.cur.t === TokType.STR) { key=this.cur.lit; this._adv(); }
          else throw ScriptError.parse(this.cur.pos, "Expected object key (identifier or string)");
          this._expect(TokType.COLON, "Expected ':' after object key"); this._adv();
          const value=this._parseCondExpr(0);
          entries.push({key, value});
          if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
          break;
        }
      }
      this._expect(TokType.RBRACE, "Expected '}'"); this._adv();
      return {kind:"ObjLit", entries, pos:p};
    }

    if (this.cur.t === TokType.LBRACK) {
      this._adv();
      const items=[];
      if (this.cur.t !== TokType.RBRACK) {
        while (true) {
          items.push(this._parseCondExpr(0));
          if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
          break;
        }
      }
      this._expect(TokType.RBRACK, "Expected ']'");
      this._adv();
      return {kind:"ArrayLit", items, pos:p};
    }

    if (this.cur.t === TokType.IDENT) {
      const id=this.cur; this._adv();
      if (this.cur.t === TokType.LPAREN) {
        this._adv();
        const args=[];
        if (this.cur.t !== TokType.RPAREN) {
          while (true) {
            args.push(this._parseCondExpr(0));
            if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
            break;
          }
        }
        this._expect(TokType.RPAREN, "Expected ')' after args"); this._adv();
        return {kind:"Call", fnNorm:String(id.lit), args, pos:id.pos};
      }
      return {kind:"Var", nameNorm:String(id.lit), pos:id.pos};
    }

    throw ScriptError.parse(p, "Expected expression");
  }

  _infixOpCond(t) {
    if (t === TokType.ASSIGN) return "==";
    return this._infixOp(t);
  }
  _parseExpr(minBp) {
    let left=this._parsePrefix();
    while (true) {
      // postfix: .prop or .method(...)
      if (this.cur.t === TokType.DOT) {
        const lbp=80;
        if (lbp < minBp) break;
        const p=this.cur.pos;
        this._adv();
        this._expect(TokType.IDENT, "Expected name after '.'");
        const nameTok=this.cur; this._adv();
        const name=nameTok.lexeme;

        if (this.cur.t === TokType.LPAREN) {
          this._adv();
          const args=[];
          if (this.cur.t !== TokType.RPAREN) {
            while (true) {
              args.push(this._parseExpr(0));
              if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
              break;
            }
          }
          this._expect(TokType.RPAREN, "Expected ')' after method args"); this._adv();
          left={kind:"MethodCall", base:left, method:name, args, pos:p};
        } else {
          left={kind:"Prop", base:left, key:name, pos:p};
        }
        continue;
      }
      // postfix: [idx]
      if (this.cur.t === TokType.LBRACK) {
        const lbp=80;
        if (lbp < minBp) break;
        const p=this.cur.pos;
        this._adv();
        const idx=this._parseExpr(0);
        this._expect(TokType.RBRACK, "Expected ']'"); this._adv();
        left={kind:"Index", base:left, idx, pos:p};
        continue;
      }
      // ternary
      if (this.cur.t === TokType.QUESTION) {
        const lbp=10;
        if (lbp < minBp) break;
        const p=this.cur.pos;
        this._adv();
        const t=this._parseExpr(0);
        this._expect(TokType.COLON, "Expected ':' in ternary"); this._adv();
        const f=this._parseExpr(0);
        left={kind:"Ternary", c:left, t, f, pos:p};
        continue;
      }

      const op=this._infixOp(this.cur.t);
      if (!op) break;
      const lbp=this._infixLbp(op);
      const rbp=lbp+1;
      if (lbp < minBp) break;
      const p=this.cur.pos;
      this._adv();
      const right=this._parseExpr(rbp);
      left={kind:"Binary", l:left, op, r:right, pos:p};
    }
    return left;
  }

  _parsePrefix() {
    const p=this.cur.pos;

    // ?(cond,a,b)
    if (this.cur.t === TokType.QUESTION) {
      const qp=this.cur.pos;
      this._adv();
      this._expect(TokType.LPAREN, "Expected '(' after '?'"); this._adv();
      const c=this._parseExpr(0);
      this._expect(TokType.COMMA, "Expected ',' after condition in ?(...)"); this._adv();
      const t=this._parseExpr(0);
      this._expect(TokType.COMMA, "Expected ',' after true-value in ?(...)"); this._adv();
      const f=this._parseExpr(0);
      this._expect(TokType.RPAREN, "Expected ')' after ?(...)"); this._adv();
      return {kind:"Ternary", c, t, f, pos:qp};
    }

    // Новый Массив / Новый Структура(...)
    if (this.cur.t === TokType.NEW) {
      const np=this.cur.pos;
      this._adv();
      if (this.cur.t === TokType.ARRAYKW) { this._adv(); return {kind:"ArrayLit", items:[], pos:np}; }
      if (this.cur.t === TokType.STRUCTKW) {
        this._adv();
        this._expect(TokType.LPAREN, "Expected '(' after 'Новый Структура'"); this._adv();
        const args=[];
        if (this.cur.t !== TokType.RPAREN) {
          while (true) {
            args.push(this._parseExpr(0));
            if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
            break;
          }
        }
        this._expect(TokType.RPAREN, "Expected ')' after Новый Структура(...)"); this._adv();
        return {kind:"Call", fnNorm:"newstructure", args, pos:np};
      }
      throw ScriptError.parse(np, "Expected 'Массив' or 'Структура' after 'Новый'");
    }

    // unary
    if (this.cur.t === TokType.MINUS) { this._adv(); return {kind:"Unary", op:"-", r:this._parseExpr(70), pos:p}; }
    if (this.cur.t === TokType.BANG) { this._adv(); return {kind:"Unary", op:"!", r:this._parseExpr(70), pos:p}; }
    if (this.cur.t === TokType.NOT) { this._adv(); return {kind:"Unary", op:"not", r:this._parseExpr(70), pos:p}; }

    // literals
    if (this.cur.t === TokType.INT) { const v=this.cur.lit; this._adv(); return {kind:"Lit", v:Value.i(v), pos:p}; }
    if (this.cur.t === TokType.FLOAT) { const v=this.cur.lit; this._adv(); return {kind:"Lit", v:Value.f(v), pos:p}; }
    if (this.cur.t === TokType.STR) { const v=this.cur.lit; this._adv(); return {kind:"Lit", v:Value.str(v), pos:p}; }
    if (this.cur.t === TokType.TRUE) { this._adv(); return {kind:"Lit", v:Value.bool(true), pos:p}; }
    if (this.cur.t === TokType.FALSE) { this._adv(); return {kind:"Lit", v:Value.bool(false), pos:p}; }
    if (this.cur.t === TokType.NULL) { this._adv(); return {kind:"Lit", v:Value.nil(), pos:p}; }

    if (this.cur.t === TokType.LPAREN) {
      this._adv();
      const e=this._parseExpr(0);
      this._expect(TokType.RPAREN, "Expected ')'"); this._adv();
      return e;
    }

    // object literal
    if (this.cur.t === TokType.LBRACE) {
      this._adv();
      const entries=[];
      if (this.cur.t !== TokType.RBRACE) {
        while (true) {
          let key;
          if (this.cur.t === TokType.IDENT) { key=this.cur.lexeme; this._adv(); }
          else if (this.cur.t === TokType.STR) { key=this.cur.lit; this._adv(); }
          else throw ScriptError.parse(this.cur.pos, "Expected object key (identifier or string)");
          this._expect(TokType.COLON, "Expected ':' after object key"); this._adv();
          const val=this._parseExpr(0);
          entries.push({key, value:val});
          if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
          break;
        }
      }
      this._expect(TokType.RBRACE, "Expected '}'"); this._adv();
      return {kind:"ObjLit", entries, pos:p};
    }

    // array literal
    if (this.cur.t === TokType.LBRACK) {
      this._adv();
      const items=[];
      if (this.cur.t !== TokType.RBRACK) {
        while (true) {
          items.push(this._parseExpr(0));
          if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
          break;
        }
      }
      this._expect(TokType.RBRACK, "Expected ']'"); this._adv();
      return {kind:"ArrayLit", items, pos:p};
    }

    if (this.cur.t === TokType.IDENT) {
      const id=this.cur; this._adv();
      if (this.cur.t === TokType.LPAREN) {
        this._adv();
        const args=[];
        if (this.cur.t !== TokType.RPAREN) {
          while (true) {
            args.push(this._parseExpr(0));
            if (this.cur.t === TokType.COMMA) { this._adv(); continue; }
            break;
          }
        }
        this._expect(TokType.RPAREN, "Expected ')' after args"); this._adv();
        return {kind:"Call", fnNorm:String(id.lit), args, pos:id.pos};
      }
      return {kind:"Var", nameNorm:String(id.lit), pos:id.pos};
    }

    throw ScriptError.parse(p, "Expected expression");
  }

  _infixOp(t) {
    if (t === TokType.PLUS) return "+";
    if (t === TokType.MINUS) return "-";
    if (t === TokType.STAR) return "*";
    if (t === TokType.SLASH) return "/";
    if (t === TokType.EQEQ) return "==";
    if (t === TokType.NOTEQ) return "!=";
    if (t === TokType.GT) return ">";
    if (t === TokType.GTE) return ">=";
    if (t === TokType.LT) return "<";
    if (t === TokType.LTE) return "<=";
    if (t === TokType.AND || t === TokType.ANDAND) return "and";
    if (t === TokType.OR || t === TokType.OROR) return "or";
    if (t === TokType.IN) return "in";
    return null;
  }
  _infixLbp(op) {
    if (op === "*" || op === "/") return 60;
    if (op === "+" || op === "-") return 50;
    if (op === ">" || op === ">=" || op === "<" || op === "<=") return 40;
    if (op === "==" || op === "!=" || op === "in") return 35;
    if (op === "and") return 25;
    if (op === "or") return 20;
    return 0;
  }

  _asLValue(e) {
    if (e.kind === "Var") return {kind:"LVar", nameNorm:e.nameNorm, pos:e.pos};
    if (e.kind === "Prop") return {kind:"LProp", base:e.base, key:e.key, pos:e.pos};
    if (e.kind === "Index") return {kind:"LIndex", base:e.base, idx:e.idx, pos:e.pos};
    return null;
  }

  _expect(t, msg) {
    if (this.cur.t !== t) throw ScriptError.parse(this.cur.pos, `${msg} (got ${this.cur.t})`);
  }
  _adv() { this.cur = this.lx.next(); }
}

/* =============================== Runtime =============================== */

class Runtime {
  constructor(dataRootValue, opts) {
    this.maxInstructions = opts.maxInstructions;
    this.tzOffsetMinutes = opts.tzOffsetMinutes;
    this.externals = opts.externals;
    this.ctx = opts.ctx ?? {};
    this.instructions = 0;
    this.scopes = [ new Map() ];
    if (!dataRootValue || dataRootValue.t !== Value.OBJ) throw new Error("_data must be object");
    this.scopes[0].set("_data", dataRootValue);
    this.builtins = new Map();
    registerBuiltins(this);
  }
  tick(pos) {
    this.instructions++;
    if (this.instructions > this.maxInstructions) {
      throw ScriptError.runtime(pos, "Execution limit exceeded: " + this.maxInstructions);
    }
  }
  pushScope() { this.scopes.unshift(new Map()); }
  popScope(pos) {
    if (this.scopes.length <= 1) throw ScriptError.runtime(pos, "Cannot pop global scope");
    this.scopes.shift();
  }
  getVar(nameNorm, pos) {
    for (const s of this.scopes) if (s.has(nameNorm)) return s.get(nameNorm);
    throw ScriptError.runtime(pos, "Variable not found: " + nameNorm);
  }
  setVar(nameNorm, v, pos) {
    if (nameNorm === "_data") throw ScriptError.runtime(pos, "_data cannot be reassigned");
    for (const s of this.scopes) {
      if (s.has(nameNorm)) { s.set(nameNorm, v); return; }
    }
    this.scopes[0].set(nameNorm, v);
  }
  resolveFn(nameNorm, pos) {
    const b = this.builtins.get(nameNorm);
    if (b) return b;
    const ext = this.externals.get(nameNorm);
    if (ext) {
      return (rt, args, p) => {
        const jsArgs = args.map(v => v.toJS());
        const res = ext(jsArgs, rt);
        return Value.fromJS(res);
      };
    }
    throw ScriptError.runtime(pos, "Unknown function: " + nameNorm);
  }
}

class ReturnSignal { constructor(v) { this.v=v; } }
class BreakSignal {}
class ContinueSignal {}
class ThrowSignal { constructor(v) { this.v=v; } }

/* =============================== Evaluator =============================== */

class Evaluator {
  constructor(rt) { this.rt=rt; }

  execProgram(program) { for (const s of program) this.exec(s); }

  execProgramGet(program) {
    try { this.execProgram(program); return Value.nil(); }
    catch (e) { if (e instanceof ReturnSignal) return e.v ?? Value.nil(); throw e; }
  }

  exec(stmt) {
    const pos = stmt.pos ?? SourcePos.UNKNOWN;
    this.rt.tick(pos);

    switch (stmt.kind) {
      case "Block":
        this.rt.pushScope();
        try { for (const s of stmt.stmts) this.exec(s); }
        finally { this.rt.popScope(pos); }
        return;

      case "Empty": return;

      case "ExprStmt": this.eval(stmt.expr); return;

      case "Assign": {
        const val=this.eval(stmt.value);
        this.assign(stmt.target, val, pos);
        return;
      }

      case "If": {
        const c=this.eval(stmt.cond);
        if (c.t !== Value.BOOL) throw ScriptError.type(pos, "IF condition expects boolean");
        if (c.v) this.exec(stmt.thenB);
        else if (stmt.elseB) this.exec(stmt.elseB);
        return;
      }

      case "Return":
        if (stmt.value === null) throw new ReturnSignal(Value.nil());
        throw new ReturnSignal(this.eval(stmt.value));

      case "While": {
        while (true) {
          const c=this.eval(stmt.cond);
          if (c.t !== Value.BOOL) throw ScriptError.type(pos, "WHILE condition expects boolean");
          if (!c.v) break;
          try { this.exec(stmt.body); }
          catch (e) {
            if (e instanceof BreakSignal) break;
            if (e instanceof ContinueSignal) continue;
            throw e;
          }
        }
        return;
      }

      case "ForRange": {
        const startV=this.eval(stmt.start);
        const endV=this.eval(stmt.end);
        if (startV.t !== Value.INT) throw ScriptError.type(pos, "For range start must be int");
        if (endV.t !== Value.INT) throw ScriptError.type(pos, "For range end must be int");
        const nameNorm=String(stmt.varName).toLowerCase();
        for (let i=startV.v;i<=endV.v;i++) {
          this.rt.setVar(nameNorm, Value.i(i), pos);
          try { this.exec(stmt.body); }
          catch (e) {
            if (e instanceof BreakSignal) break;
            if (e instanceof ContinueSignal) continue;
            throw e;
          }
        }
        return;
      }

      case "ForIn": {
        const it=this.eval(stmt.iter);
        if (it.t !== Value.ARR) throw ScriptError.type(pos, "FOR-IN expects array");
        const nameNorm=String(stmt.varName).toLowerCase();
        for (let i=0;i<it.v.length;i++) {
          this.rt.setVar(nameNorm, Value.fromJS(it.v[i]), pos);
          try { this.exec(stmt.body); }
          catch (e) {
            if (e instanceof BreakSignal) break;
            if (e instanceof ContinueSignal) continue;
            throw e;
          }
        }
        return;
      }

      case "ForC": {
        this.exec(stmt.init);
        while (true) {
          const c=this.eval(stmt.cond);
          if (c.t !== Value.BOOL) throw ScriptError.type(pos, "FOR condition expects boolean");
          if (!c.v) break;
          let doUpdate = true;
          try { this.exec(stmt.body); }
          catch (e) {
            if (e instanceof BreakSignal) { doUpdate = false; break; }
            if (e instanceof ContinueSignal) { /* still do update */ }
            else throw e;
          }
          if (doUpdate) this.exec(stmt.update);
        }
        return;
      }

      case "TryExcept": {
        try { this.exec(stmt.tryB); }
        catch (e) {
          const errVal = (e instanceof ThrowSignal) ? e.v : Value.str(String(e));
          this.rt.pushScope();
          try {
            this.rt.setVar(stmt.errNorm, errVal, pos);
            this.exec(stmt.exceptB);
          } finally { this.rt.popScope(pos); }
        }
        return;
      }

      case "Throw": throw new ThrowSignal(this.eval(stmt.value));
      case "Break": throw new BreakSignal();
      case "Continue": throw new ContinueSignal();

      default: throw ScriptError.runtime(pos, "Unknown statement kind: " + stmt.kind);
    }
  }

  assign(lv, val, pos) {
    if (lv.kind === "LVar") { this.rt.setVar(lv.nameNorm, val, pos); return; }

    if (lv.kind === "LProp") {
      const base=this.eval(lv.base);
      if (base.t === Value.NIL) throw ScriptError.runtime(pos, "Null reference on assignment to .prop");
      if (base.t !== Value.OBJ) throw ScriptError.type(pos, "Property assignment expects object");
      base.v[lv.key] = val.toJS();
      return;
    }

    if (lv.kind === "LIndex") {
      const base=this.eval(lv.base);
      const idx=this.eval(lv.idx);
      if (base.t === Value.NIL) throw ScriptError.runtime(pos, "Null reference on assignment to []");
      if (base.t !== Value.ARR) throw ScriptError.type(pos, "Index assignment expects array");
      if (idx.t !== Value.INT) throw ScriptError.type(pos, "Array index must be int");
      const n=idx.v;
      if (n < 0 || n >= base.v.length) throw ScriptError.runtime(pos, "Array index out of range: " + n);
      base.v[n] = val.toJS();
      return;
    }

    throw ScriptError.runtime(pos, "Invalid assignment target");
  }

  eval(expr) {
    const pos = expr.pos ?? SourcePos.UNKNOWN;
    this.rt.tick(pos);

    switch (expr.kind) {
      case "Lit": return expr.v;
      case "Var": return this.rt.getVar(expr.nameNorm, pos);

      case "Unary": {
        const r=this.eval(expr.r);
        if (expr.op === "-") return Value.neg(r, pos);
        if (expr.op === "!" || expr.op === "not") return Value.not(r, pos);
        throw ScriptError.runtime(pos, "Unknown unary op: " + expr.op);
      }

      case "Binary": {
        const a=this.eval(expr.l);
        const b=this.eval(expr.r);
        switch (expr.op) {
          case "+": return Value.add(a,b,pos);
          case "-": return Value.sub(a,b,pos);
          case "*": return Value.mul(a,b,pos);
          case "/": return Value.div(a,b,pos);
          case "==": return Value.eq(a,b);
          case "!=": return Value.ne(a,b);
          case ">":
          case ">=":
          case "<":
          case "<=": return Value.cmp(a,b,expr.op,pos);
          case "and": return Value.and(a,b,pos);
          case "or": return Value.or(a,b,pos);
          case "in": return Value.inOp(a,b,pos);
          default: throw ScriptError.runtime(pos, "Unknown binary op: " + expr.op);
        }
      }

      case "Ternary": {
        const c=this.eval(expr.c);
        if (c.t !== Value.BOOL) throw ScriptError.type(pos, "Ternary condition expects boolean");
        return c.v ? this.eval(expr.t) : this.eval(expr.f);
      }

      case "Prop": {
        const base=this.eval(expr.base);
        if (base.t === Value.NIL) return Value.nil();
        if (base.t !== Value.OBJ) throw ScriptError.type(pos, "Property access expects object");
        return Value.fromJS(base.v[expr.key]);
      }

      case "Index": {
        const base=this.eval(expr.base);
        const idx=this.eval(expr.idx);
        if (base.t === Value.NIL) return Value.nil();
        if (base.t === Value.ARR) {
          if (idx.t !== Value.INT) throw ScriptError.type(pos, "Array index must be int");
          const n=idx.v;
          if (n < 0 || n >= base.v.length) throw ScriptError.runtime(pos, "Array index out of range: " + n);
          return Value.fromJS(base.v[n]);
        }
        if (base.t === Value.OBJ) {
          if (idx.t !== Value.STR) throw ScriptError.type(pos, "Object index expects string");
          return Value.fromJS(base.v[idx.v]);
        }
        throw ScriptError.type(pos, "Indexing expects array or object");
      }

      case "Call": {
        const fn=this.rt.resolveFn(expr.fnNorm, pos);
        const args=expr.args.map(a=>this.eval(a));
        return fn(this.rt, args, pos);
      }

      case "MethodCall": {
        const base=this.eval(expr.base);
        const args=expr.args.map(a=>this.eval(a));
        return evalMethodCall(base, expr.method, args, pos);
      }

      case "ArrayLit": return Value.arr(expr.items.map(it=>this.eval(it).toJS()));

      case "ObjLit": {
        const o={};
        for (const e of expr.entries) o[e.key] = this.eval(e.value).toJS();
        return Value.obj(o);
      }

      default: throw ScriptError.runtime(pos, "Unknown expr kind: " + expr.kind);
    }
  }
}

/* =============================== Methods =============================== */

function evalMethodCall(base, methodName, args, pos) {
  const ml=String(methodName ?? "").toLowerCase();

  if (base.t === Value.ARR) {
    const a=base.v;
    if (ml === "add") {
      if (args.length !== 1) throw ScriptError.type(pos, "arr.add expects 1 arg");
      a.push(args[0].toJS());
      return Value.nil();
    }
    if (ml === "clear") {
      if (args.length !== 0) throw ScriptError.type(pos, "arr.clear expects 0 args");
      a.length = 0;
      return Value.nil();
    }
    if (ml === "contains") {
      if (args.length !== 1) throw ScriptError.type(pos, "arr.contains expects 1 arg");
      for (let i=0;i<a.length;i++) if (Value.eq(Value.fromJS(a[i]), args[0]).v) return Value.bool(true);
      return Value.bool(false);
    }
  }

  if (base.t === Value.OBJ) {
    const o=base.v;
    if (ml === "has") {
      if (args.length !== 1) throw ScriptError.type(pos, "obj.has expects 1 arg");
      const k=args[0].asStr(pos);
      return Value.bool(Object.prototype.hasOwnProperty.call(o,k));
    }
  }

  if (base.t === Value.STR) {
    const s=base.v;
    if (ml === "contains") {
      if (args.length !== 1) throw ScriptError.type(pos, "str.contains expects 1 arg");
      const sub=args[0].asStr(pos);
      return Value.bool(s.indexOf(sub) >= 0);
    }
  }

  throw ScriptError.runtime(pos, `Unknown method '${methodName}' for type`);
}

/* =============================== Builtins =============================== */
function httpRequestSync(method, url, params, body, headers, auth, pos) {
  const finalUrl = buildHttpUrl(url, params);
  const xhr = new XMLHttpRequest();

  try {
    xhr.open(method, finalUrl, false); // sync

    // headers
    if (headers && typeof headers === "object" && !Array.isArray(headers)) {
      for (const k of Object.keys(headers)) {
        const v = headers[k];
        if (v === null || v === undefined) continue;
        xhr.setRequestHeader(k, String(v));
      }
    }

    // auth
    const authPair = normalizeHttpAuth(auth);
    if (authPair) {
      xhr.setRequestHeader("Authorization", "Basic " + btoa(authPair.user + ":" + authPair.pass));
    }

    let payload = null;

    if (body !== null && body !== undefined) {
      if (typeof body === "string") {
        payload = body;
      } else if (typeof body === "object") {
        payload = JSON.stringify(body);

        let hasContentType = false;
        if (headers && typeof headers === "object" && !Array.isArray(headers)) {
          for (const k of Object.keys(headers)) {
            if (String(k).toLowerCase() === "content-type") {
              hasContentType = true;
              break;
            }
          }
        }
        if (!hasContentType) {
          xhr.setRequestHeader("Content-Type", "application/json; charset=UTF-8");
        }
      } else {
        payload = String(body);
      }
    }

    xhr.send(payload);

    const text = xhr.responseText ?? "";
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (_) {}

    return {
      ok: xhr.status >= 200 && xhr.status < 300,
      status: xhr.status || 0,
      headers: parseResponseHeaders(xhr.getAllResponseHeaders()),
      text: text,
      json: json
    };
  } catch (e) {
    return {
      ok: false,
      status: 0,
      headers: {},
      text: String(e && e.message ? e.message : e),
      json: null
    };
  }
}

function buildHttpUrl(url, params) {
  const u = new URL(String(url), window.location.origin);

  if (params && typeof params === "object" && !Array.isArray(params)) {
    for (const k of Object.keys(params)) {
      const v = params[k];
      if (v === null || v === undefined) continue;
      u.searchParams.append(k, String(v));
    }
  }

  return u.toString();
}

function normalizeHttpAuth(auth) {
  if (auth === null || auth === undefined) return null;

  if (typeof auth === "string") {
    const p = auth.indexOf(":");
    if (p < 0) return { user: auth, pass: "" };
    return {
      user: auth.slice(0, p),
      pass: auth.slice(p + 1)
    };
  }

  if (typeof auth === "object") {
    const user = auth.user ?? auth.username ?? "";
    const pass = auth.pass ?? auth.password ?? "";
    return { user: String(user), pass: String(pass) };
  }

  return null;
}

function parseResponseHeaders(raw) {
  const out = {};
  const text = String(raw || "");
  const lines = text.split(/\r?\n/);
  for (const line of lines) {
    if (!line) continue;
    const p = line.indexOf(":");
    if (p <= 0) continue;
    const k = line.slice(0, p).trim();
    const v = line.slice(p + 1).trim();
    out[k] = v;
  }
  return out;
}

function registerBuiltins(rt) {
  const put = (name, fn) => rt.builtins.set(name, fn);

  put("now", (rt,args,pos)=>{ arity(args,0,pos,"Now"); return Value.date(Date.now()); });

  put("parsedate", (rt,args,pos)=>{
    if (!(args.length === 1 || args.length === 2)) throw ScriptError.type(pos, "ParseDate expects 1 or 2 args");
    const text=reqStr(args[0],pos,"ParseDate(text[,pattern])");
    const pattern=args.length===2 ? reqStr(args[1],pos,"ParseDate(text,pattern)") : null;
    return Value.date(parseDateImpl(text, pattern, rt.tzOffsetMinutes, pos));
  });

  put("formatdate", (rt,args,pos)=>{
    arity(args,2,pos,"FormatDate");
    if (args[0].t !== Value.DATE) throw ScriptError.type(pos, "FormatDate expects DATE");
    return Value.str(formatDateImpl(args[0].v, reqStr(args[1],pos,"FormatDate(date,pattern)"), rt.tzOffsetMinutes));
  });

  put("adddays", (rt,args,pos)=>{ arity(args,2,pos,"AddDays"); return Value.date(addCalendar(reqDate(args[0],pos,"AddDays(date,days)"), "day", reqInt(args[1],pos,"AddDays(date,days)"), rt.tzOffsetMinutes)); });
  put("addmonths",(rt,args,pos)=>{ arity(args,2,pos,"AddMonths"); return Value.date(addCalendar(reqDate(args[0],pos,"AddMonths(date,months)"), "month", reqInt(args[1],pos,"AddMonths(date,months)"), rt.tzOffsetMinutes)); });
  put("addyears", (rt,args,pos)=>{ arity(args,2,pos,"AddYears"); return Value.date(addCalendar(reqDate(args[0],pos,"AddYears(date,years)"), "year", reqInt(args[1],pos,"AddYears(date,years)"), rt.tzOffsetMinutes)); });

  put("startofday",(rt,args,pos)=>{ arity(args,1,pos,"StartOfDay"); return Value.date(startOfDay(reqDate(args[0],pos,"StartOfDay(date)"), rt.tzOffsetMinutes)); });
  put("endofday",(rt,args,pos)=>{ arity(args,1,pos,"EndOfDay"); return Value.date(endOfDay(reqDate(args[0],pos,"EndOfDay(date)"), rt.tzOffsetMinutes)); });
  put("startofmonth",(rt,args,pos)=>{ arity(args,1,pos,"StartOfMonth"); return Value.date(startOfMonth(reqDate(args[0],pos,"StartOfMonth(date)"), rt.tzOffsetMinutes)); });
  put("endofmonth",(rt,args,pos)=>{ arity(args,1,pos,"EndOfMonth"); return Value.date(endOfMonth(reqDate(args[0],pos,"EndOfMonth(date)"), rt.tzOffsetMinutes)); });
  put("startofyear",(rt,args,pos)=>{ arity(args,1,pos,"StartOfYear"); return Value.date(startOfYear(reqDate(args[0],pos,"StartOfYear(date)"), rt.tzOffsetMinutes)); });
  put("endofyear",(rt,args,pos)=>{ arity(args,1,pos,"EndOfYear"); return Value.date(endOfYear(reqDate(args[0],pos,"EndOfYear(date)"), rt.tzOffsetMinutes)); });

  put("string",(rt,args,pos)=>{ arity(args,1,pos,"String"); return Value.str(stringStrict(args[0],pos)); });
  put("length",(rt,args,pos)=>{ arity(args,1,pos,"Length"); return args[0].length(pos); });

  put("строка", rt.builtins.get("string"));
  put("стрдлина", rt.builtins.get("length"));

  put("началодня", rt.builtins.get("startofday"));
  put("конецдня", rt.builtins.get("endofday"));
  put("началомесяца", rt.builtins.get("startofmonth"));
  put("конецмесяца", rt.builtins.get("endofmonth"));
  put("началогода", rt.builtins.get("startofyear"));
  put("сконецгодаа", rt.builtins.get("endofyear"));

  put("текущаядата", rt.builtins.get("eor"));

  put("substring",(rt,args,pos)=>{
    arity(args,3,pos,"Substring");
    const s=reqStr(args[0],pos,"Substring(str,start,len)");
    const start=reqInt(args[1],pos,"Substring(str,start,len)");
    const len=reqInt(args[2],pos,"Substring(str,start,len)");
    if (start < 0 || len < 0 || start > s.length || (start+len) > s.length) throw ScriptError.runtime(pos,"Substring out of range");
    return Value.str(s.substring(start, start+len));
  });

  put("indexof",(rt,args,pos)=>{
    arity(args,2,pos,"IndexOf");
    return Value.i(reqStr(args[0],pos,"IndexOf(str,sub)").indexOf(reqStr(args[1],pos,"IndexOf(str,sub)")));
  });

  put("hasproperty",(rt,args,pos)=>{
    arity(args,2,pos,"HasProperty");
    const obj=args[0];
    const key=reqStr(args[1],pos,"HasProperty(obj,key)");
    if (obj.t !== Value.OBJ) throw ScriptError.type(pos,"HasProperty expects object");
    return Value.bool(Object.prototype.hasOwnProperty.call(obj.v, key));
  });
  put("свойство", rt.builtins.get("hasproperty"));

  put("httprequest",(rt,args,pos)=>{
    if (args.length < 4 || args.length > 6) {
      throw ScriptError.type(pos, "HTTPRequest(method, url, params, body [, headers [, auth]])");
    }

    const method = reqStr(args[0], pos, "HTTPRequest(method,url,params,body[,headers[,auth]])").toUpperCase();
    const url = reqStr(args[1], pos, "HTTPRequest(method,url,params,body[,headers[,auth]])");
    const params = args[2].toJS();
    const body = args[3].toJS();
    const headers = args.length >= 5 ? args[4].toJS() : null;
    const auth = args.length >= 6 ? args[5].toJS() : null;

    const result = httpRequestSync(method, url, params, body, headers, auth, pos);
    return Value.fromJS(result);
  });

  put("httpзапрос", rt.builtins.get("httprequest"));

  put("newarray",(rt,args,pos)=>{ arity(args,0,pos,"NewArray"); return Value.arr([]); });
  put("newobject",(rt,args,pos)=>{ arity(args,0,pos,"NewObject"); return Value.obj({}); });
  put("newstructure",(rt,args,pos)=>{
    if ((args.length % 2) !== 0) throw ScriptError.type(pos,"NewStructure expects even arg count");
    const o={};
    for (let i=0;i<args.length;i+=2) o[reqStr(args[i],pos,"NewStructure(key,value,...)")] = args[i+1].toJS();
    return Value.obj(o);
  });
  put("новаяструктура", rt.builtins.get("newstructure"));
  put("структура", rt.builtins.get("newstructure"));
}

/* =============================== Builtin helpers =============================== */

function arity(args, n, pos, name) {
  if (args.length !== n) throw ScriptError.type(pos, `${name} expects ${n} args`);
}
function reqStr(v, pos, sig) { if (v.t !== Value.STR) throw ScriptError.type(pos, `${sig} expects string`); return v.v; }
function reqInt(v, pos, sig) { if (v.t !== Value.INT) throw ScriptError.type(pos, `${sig} expects int`); return v.v; }
function reqDate(v, pos, sig) { if (v.t !== Value.DATE) throw ScriptError.type(pos, `${sig} expects date`); return v.v; }

function stringStrict(v, pos) {
  if (v.t === Value.NIL) return "null";
  if (v.t === Value.BOOL) return v.v ? "true" : "false";
  if (v.t === Value.INT || v.t === Value.FLOAT) return String(v.v);
  if (v.t === Value.STR) return v.v;
  if (v.t === Value.DATE) return String(v.v);
  if (v.t === Value.ARR) return "Array(" + v.v.length + ")";
  if (v.t === Value.OBJ) return "Object(" + Object.keys(v.v ?? {}).length + ")";
  throw ScriptError.type(pos, "Cannot stringify given type");
}

function getOffsetMinutes(tzOffsetMinutes) {
  if (tzOffsetMinutes === null || tzOffsetMinutes === undefined) {
    return -new Date().getTimezoneOffset(); // minutes east of UTC
  }
  return Number(tzOffsetMinutes);
}
function shiftToTz(ms, tzOffsetMinutes) { const off=getOffsetMinutes(tzOffsetMinutes); return ms + off*60_000; }
function shiftFromTz(msLocal, tzOffsetMinutes) { const off=getOffsetMinutes(tzOffsetMinutes); return msLocal - off*60_000; }

function startOfDay(ms, tzOffsetMinutes) { const local=shiftToTz(ms,tzOffsetMinutes); const d=new Date(local); d.setUTCHours(0,0,0,0); return shiftFromTz(d.getTime(),tzOffsetMinutes); }
function endOfDay(ms, tzOffsetMinutes) { return startOfDay(ms,tzOffsetMinutes) + 24*60*60*1000 - 1; }
function startOfMonth(ms, tzOffsetMinutes) { const local=shiftToTz(ms,tzOffsetMinutes); const d=new Date(local); d.setUTCDate(1); d.setUTCHours(0,0,0,0); return shiftFromTz(d.getTime(),tzOffsetMinutes); }
function endOfMonth(ms, tzOffsetMinutes) { const local=shiftToTz(ms,tzOffsetMinutes); const d=new Date(local); d.setUTCDate(1); d.setUTCMonth(d.getUTCMonth()+1); d.setUTCHours(0,0,0,0); return shiftFromTz(d.getTime(),tzOffsetMinutes)-1; }
function startOfYear(ms, tzOffsetMinutes) { const local=shiftToTz(ms,tzOffsetMinutes); const d=new Date(local); d.setUTCMonth(0,1); d.setUTCHours(0,0,0,0); return shiftFromTz(d.getTime(),tzOffsetMinutes); }
function endOfYear(ms, tzOffsetMinutes) { const local=shiftToTz(ms,tzOffsetMinutes); const d=new Date(local); d.setUTCMonth(0,1); d.setUTCFullYear(d.getUTCFullYear()+1); d.setUTCHours(0,0,0,0); return shiftFromTz(d.getTime(),tzOffsetMinutes)-1; }

function addCalendar(ms, unit, amount, tzOffsetMinutes) {
  const local=shiftToTz(ms,tzOffsetMinutes);
  const d=new Date(local);
  if (unit === "day") d.setUTCDate(d.getUTCDate()+amount);
  else if (unit === "month") d.setUTCMonth(d.getUTCMonth()+amount);
  else if (unit === "year") d.setUTCFullYear(d.getUTCFullYear()+amount);
  else throw new Error("Bad unit");
  return shiftFromTz(d.getTime(), tzOffsetMinutes);
}

function parseDateImpl(text, pattern, tzOffsetMinutes, pos) {
  const t=String(text ?? "").trim();
  if (!t) throw ScriptError.runtime(pos, "ParseDate: text is blank");
  if (/^\d+$/.test(t)) {
    const n=Number(t);
    if (!Number.isFinite(n)) throw ScriptError.runtime(pos, "ParseDate: bad epochMillis");
    return n;
  }
  if (pattern) {
    const ms=parseWithPattern(t, String(pattern), tzOffsetMinutes);
    if (ms !== null) return ms;
    throw ScriptError.runtime(pos, "ParseDate: unsupported pattern: " + pattern);
  }
  const parsed=Date.parse(t);
  if (Number.isFinite(parsed)) return parsed;

  const m=/^(\d{4})-(\d{2})-(\d{2})$/.exec(t);
  if (m) {
    const y=+m[1], mo=+m[2]-1, d=+m[3];
    const local=Date.UTC(y,mo,d,0,0,0,0);
    return shiftFromTz(local, tzOffsetMinutes);
  }
  throw ScriptError.runtime(pos, "ParseDate: cannot parse: " + t);
}

function parseWithPattern(text, pattern, tzOffsetMinutes) {
  const t=text.trim();
  const mk=(y,mo,d,hh,mm,ss,ms)=>shiftFromTz(Date.UTC(y,mo,d,hh,mm,ss,ms), tzOffsetMinutes);

  if (pattern === "dd.MM.yyyy") {
    const m=/^(\d{2})\.(\d{2})\.(\d{4})$/.exec(t);
    if (!m) return null;
    return mk(+m[3], +m[2]-1, +m[1], 0,0,0,0);
  }
  if (pattern === "dd.MM.yyyy HH:mm") {
    const m=/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})$/.exec(t);
    if (!m) return null;
    return mk(+m[3], +m[2]-1, +m[1], +m[4], +m[5], 0, 0);
  }
  if (pattern === "yyyy-MM-dd") {
    const m=/^(\d{4})-(\d{2})-(\d{2})$/.exec(t);
    if (!m) return null;
    return mk(+m[1], +m[2]-1, +m[3], 0,0,0,0);
  }
  if (pattern === "yyyy-MM-dd'T'HH:mm:ss") {
    const m=/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/.exec(t);
    if (!m) return null;
    return mk(+m[1], +m[2]-1, +m[3], +m[4], +m[5], +m[6], 0);
  }
  if (pattern === "yyyy-MM-dd'T'HH:mm:ss.SSS") {
    const m=/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})$/.exec(t);
    if (!m) return null;
    return mk(+m[1], +m[2]-1, +m[3], +m[4], +m[5], +m[6], +m[7]);
  }

  const parsed=Date.parse(t);
  if (Number.isFinite(parsed)) return parsed;
  return null;
}

function pad2(n) { return String(n).padStart(2, "0"); }
function pad3(n) { return String(n).padStart(3, "0"); }

function formatDateImpl(ms, pattern, tzOffsetMinutes) {
  const local=shiftToTz(ms, tzOffsetMinutes);
  const d=new Date(local);
  const yyyy=d.getUTCFullYear();
  const MM=pad2(d.getUTCMonth()+1);
  const dd=pad2(d.getUTCDate());
  const HH=pad2(d.getUTCHours());
  const mm=pad2(d.getUTCMinutes());
  const ss=pad2(d.getUTCSeconds());
  const SSS=pad3(d.getUTCMilliseconds());

  let out=String(pattern);
  out=out.replace(/yyyy/g, String(yyyy));
  out=out.replace(/SSS/g, SSS);
  out=out.replace(/MM/g, MM);
  out=out.replace(/dd/g, dd);
  out=out.replace(/HH/g, HH);
  out=out.replace(/mm/g, mm);
  out=out.replace(/ss/g, ss);
  return out;
}

// expose globally
try { window.NodaScriptEngine = NodaScriptEngine; } catch(_) {}

