//! Lexer, parser and AST for DynamoDB expressions: conditions/filters/key conditions,
//! projections and update expressions.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

static RESERVED: &str = include_str!("reserved_keywords.txt");

pub fn is_reserved(word: &str) -> bool {
    RESERVED.lines().any(|w| w.eq_ignore_ascii_case(word))
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathElem {
    Attr(String),
    Index(usize),
}

pub type Path = Vec<PathElem>;

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Path(Path),
    /// `:name` (resolved against `ExpressionAttributeValues` at evaluation time).
    Placeholder(String),
    Func(String, Vec<Operand>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cond {
    Compare(CmpOp, Operand, Operand),
    Between(Operand, Operand, Operand),
    In(Operand, Vec<Operand>),
    And(Box<Cond>, Box<Cond>),
    Or(Box<Cond>, Box<Cond>),
    Not(Box<Cond>),
    /// A boolean function used as a whole condition (`attribute_exists(a)`, `begins_with(a, :p)`).
    Function(String, Vec<Operand>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetValue {
    Operand(Operand),
    Plus(Operand, Operand),
    Minus(Operand, Operand),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpdateExpr {
    pub set: Vec<(Path, SetValue)>,
    pub remove: Vec<Path>,
    pub add: Vec<(Path, Operand)>,
    pub delete: Vec<(Path, Operand)>,
}

/// `ExpressionAttributeNames` / `ExpressionAttributeValues` plus usage tracking for the
/// "unused" validation DynamoDB performs.
#[derive(Debug, Default)]
pub struct Env<'a> {
    pub names: Option<&'a BTreeMap<String, String>>,
    pub values: Option<&'a Map<String, Value>>,
    pub used_names: BTreeSet<String>,
    pub used_values: BTreeSet<String>,
}

impl<'a> Env<'a> {
    pub fn new(
        names: Option<&'a BTreeMap<String, String>>,
        values: Option<&'a Map<String, Value>>,
    ) -> Self {
        Self {
            names,
            values,
            used_names: BTreeSet::new(),
            used_values: BTreeSet::new(),
        }
    }

    /// Error text when supplied names/values were never referenced.
    pub fn unused_error(&self) -> Option<String> {
        let unused = |all: Vec<&String>, used: &BTreeSet<String>| -> Vec<String> {
            all.into_iter()
                .filter(|k| !used.contains(*k))
                .cloned()
                .collect()
        };
        if let Some(names) = self.names {
            let u = unused(names.keys().collect(), &self.used_names);
            if !u.is_empty() {
                return Some(format!(
                    "Value provided in ExpressionAttributeNames unused in expressions: keys: {{{}}}",
                    u.join(", ")
                ));
            }
        }
        if let Some(values) = self.values {
            let u = unused(values.keys().collect(), &self.used_values);
            if !u.is_empty() {
                return Some(format!(
                    "Value provided in ExpressionAttributeValues unused in expressions: keys: {{{}}}",
                    u.join(", ")
                ));
            }
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    NameRef(String),
    ValueRef(String),
    Num(usize),
    Sym(char),
    Op(CmpOp),
    End,
}

fn lex(src: &str, what: &str) -> Result<Vec<(Tok, usize)>, String> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let syntax = |tok: &str, at: usize| {
        format!(
            "Invalid {what}: Syntax error; token: \"{tok}\", near: \"{}\"",
            chars[at..].iter().take(10).collect::<String>()
        )
    };
    while i < chars.len() {
        let c = chars[i];
        let start = i;
        match c {
            c if c.is_whitespace() => i += 1,
            '(' | ')' | ',' | '.' | '[' | ']' | '+' | '-' => {
                out.push((Tok::Sym(c), start));
                i += 1;
            }
            '=' => {
                out.push((Tok::Op(CmpOp::Eq), start));
                i += 1;
            }
            '<' => {
                i += 1;
                match chars.get(i) {
                    Some('>') => {
                        out.push((Tok::Op(CmpOp::Ne), start));
                        i += 1;
                    }
                    Some('=') => {
                        out.push((Tok::Op(CmpOp::Le), start));
                        i += 1;
                    }
                    _ => out.push((Tok::Op(CmpOp::Lt), start)),
                }
            }
            '>' => {
                i += 1;
                if chars.get(i) == Some(&'=') {
                    out.push((Tok::Op(CmpOp::Ge), start));
                    i += 1;
                } else {
                    out.push((Tok::Op(CmpOp::Gt), start));
                }
            }
            '#' | ':' => {
                i += 1;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                if text.len() == 1 {
                    return Err(syntax(&text, start));
                }
                out.push((
                    if c == '#' {
                        Tok::NameRef(text)
                    } else {
                        Tok::ValueRef(text)
                    },
                    start,
                ));
            }
            c if c.is_ascii_digit() => {
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                out.push((
                    Tok::Num(text.parse().map_err(|_| syntax(&text, start))?),
                    start,
                ));
            }
            c if c.is_alphabetic() || c == '_' => {
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                out.push((Tok::Ident(chars[start..i].iter().collect()), start));
            }
            other => return Err(syntax(&other.to_string(), start)),
        }
    }
    out.push((Tok::End, chars.len()));
    Ok(out)
}

struct Parser<'e, 'a> {
    toks: Vec<(Tok, usize)>,
    pos: usize,
    src: String,
    what: &'static str,
    env: &'e mut Env<'a>,
}

const KEYWORDS: [&str; 9] = [
    "AND", "OR", "NOT", "BETWEEN", "IN", "SET", "REMOVE", "ADD", "DELETE",
];

impl<'e, 'a> Parser<'e, 'a> {
    fn new(src: &str, what: &'static str, env: &'e mut Env<'a>) -> Result<Self, String> {
        Ok(Self {
            toks: lex(src, what)?,
            pos: 0,
            src: src.to_string(),
            what,
            env,
        })
    }

    fn peek(&self) -> &Tok {
        &self.toks[self.pos].0
    }

    fn bump(&mut self) -> Tok {
        let t = self.toks[self.pos].0.clone();
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
        t
    }

    fn err(&self) -> String {
        let (tok, at) = &self.toks[self.pos];
        let text = match tok {
            Tok::End => "<EOF>".to_string(),
            Tok::Ident(s) | Tok::NameRef(s) | Tok::ValueRef(s) => s.clone(),
            Tok::Num(n) => n.to_string(),
            Tok::Sym(c) => c.to_string(),
            Tok::Op(_) => self.src.chars().skip(*at).take(1).collect(),
        };
        let near: String = self
            .src
            .chars()
            .skip(at.saturating_sub(0))
            .take(12)
            .collect();
        format!(
            "Invalid {}: Syntax error; token: \"{text}\", near: \"{near}\"",
            self.what
        )
    }

    fn eat_sym(&mut self, c: char) -> bool {
        if self.peek() == &Tok::Sym(c) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_sym(&mut self, c: char) -> Result<(), String> {
        if self.eat_sym(c) {
            Ok(())
        } else {
            Err(self.err())
        }
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if matches!(self.peek(), Tok::Ident(s) if s.eq_ignore_ascii_case(kw)) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn at_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Tok::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn resolve_name(&mut self, reference: &str) -> Result<String, String> {
        match self.env.names.and_then(|n| n.get(reference)) {
            Some(n) => {
                self.env.used_names.insert(reference.to_string());
                Ok(n.clone())
            }
            None => Err(format!(
                "Invalid {}: An expression attribute name used in the document path is not defined; attribute name: {reference}",
                self.what
            )),
        }
    }

    fn attr_name(&mut self) -> Result<String, String> {
        match self.bump() {
            Tok::NameRef(r) => self.resolve_name(&r),
            Tok::Ident(w) => {
                if is_reserved(&w) || KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(&w)) {
                    return Err(format!(
                        "Invalid {}: Attribute name is a reserved keyword; reserved keyword: {w}",
                        self.what
                    ));
                }
                Ok(w)
            }
            _ => {
                self.pos = self.pos.saturating_sub(1);
                Err(self.err())
            }
        }
    }

    fn path(&mut self) -> Result<Path, String> {
        let mut path = vec![PathElem::Attr(self.attr_name()?)];
        loop {
            if self.eat_sym('.') {
                path.push(PathElem::Attr(self.attr_name()?));
            } else if self.eat_sym('[') {
                match self.bump() {
                    Tok::Num(n) => path.push(PathElem::Index(n)),
                    _ => {
                        self.pos = self.pos.saturating_sub(1);
                        return Err(self.err());
                    }
                }
                self.expect_sym(']')?;
            } else {
                return Ok(path);
            }
        }
    }

    fn operand(&mut self) -> Result<Operand, String> {
        match self.peek().clone() {
            Tok::ValueRef(v) => {
                self.bump();
                match self.env.values {
                    Some(vals) if vals.contains_key(&v) => {
                        self.env.used_values.insert(v.clone());
                        Ok(Operand::Placeholder(v))
                    }
                    _ => Err(format!(
                        "Invalid {}: An expression attribute value used in expression is not defined; attribute value: {v}",
                        self.what
                    )),
                }
            }
            Tok::Ident(name)
                if self.toks.get(self.pos + 1).map(|t| &t.0) == Some(&Tok::Sym('(')) =>
            {
                self.bump();
                self.bump();
                let mut args = Vec::new();
                if !self.eat_sym(')') {
                    loop {
                        args.push(self.operand()?);
                        if self.eat_sym(')') {
                            break;
                        }
                        self.expect_sym(',')?;
                    }
                }
                Ok(Operand::Func(name, args))
            }
            Tok::Ident(_) | Tok::NameRef(_) => Ok(Operand::Path(self.path()?)),
            _ => Err(self.err()),
        }
    }

    // ---- conditions ----
    fn or(&mut self) -> Result<Cond, String> {
        let mut left = self.and()?;
        while self.eat_kw("OR") {
            left = Cond::Or(Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Cond, String> {
        let mut left = self.not()?;
        while self.eat_kw("AND") {
            left = Cond::And(Box::new(left), Box::new(self.not()?));
        }
        Ok(left)
    }

    fn not(&mut self) -> Result<Cond, String> {
        if self.eat_kw("NOT") {
            return Ok(Cond::Not(Box::new(self.not()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Cond, String> {
        if self.peek() == &Tok::Sym('(') {
            self.bump();
            let c = self.or()?;
            self.expect_sym(')')?;
            return Ok(c);
        }
        let left = self.operand()?;
        let bool_fn = matches!(&left, Operand::Func(n, _) if matches!(n.as_str(), "attribute_exists" | "attribute_not_exists" | "attribute_type" | "begins_with" | "contains"));
        if bool_fn
            && !matches!(self.peek(), Tok::Op(_))
            && !self.at_kw("BETWEEN")
            && !self.at_kw("IN")
        {
            if let Operand::Func(n, a) = left {
                return Ok(Cond::Function(n, a));
            }
        }
        if let Tok::Op(op) = self.peek().clone() {
            self.bump();
            let right = self.operand()?;
            return Ok(Cond::Compare(op, left, right));
        }
        if self.eat_kw("BETWEEN") {
            let lo = self.operand()?;
            if !self.eat_kw("AND") {
                return Err(self.err());
            }
            let hi = self.operand()?;
            return Ok(Cond::Between(left, lo, hi));
        }
        if self.eat_kw("IN") {
            self.expect_sym('(')?;
            let mut items = vec![self.operand()?];
            while self.eat_sym(',') {
                items.push(self.operand()?);
            }
            self.expect_sym(')')?;
            return Ok(Cond::In(left, items));
        }
        if let Operand::Func(name, _) = &left {
            return Err(format!(
                "Invalid {}: Invalid function name; function: {name}",
                self.what
            ));
        }
        Err(self.err())
    }

    // ---- update ----
    fn set_value(&mut self) -> Result<SetValue, String> {
        let a = self.operand()?;
        if self.eat_sym('+') {
            return Ok(SetValue::Plus(a, self.operand()?));
        }
        if self.eat_sym('-') {
            return Ok(SetValue::Minus(a, self.operand()?));
        }
        Ok(SetValue::Operand(a))
    }

    fn update(&mut self) -> Result<UpdateExpr, String> {
        let mut u = UpdateExpr::default();
        let (mut seen_set, mut seen_remove, mut seen_add, mut seen_delete) =
            (false, false, false, false);
        let dup = |w: &str, what: &str| {
            format!(
                "Invalid {what}: The \"{w}\" section can only be used once in an update expression;"
            )
        };
        while *self.peek() != Tok::End {
            if self.eat_kw("SET") {
                if std::mem::replace(&mut seen_set, true) {
                    return Err(dup("SET", self.what));
                }
                loop {
                    let p = self.path()?;
                    if self.peek() != &Tok::Op(CmpOp::Eq) {
                        return Err(self.err());
                    }
                    self.bump();
                    u.set.push((p, self.set_value()?));
                    if !self.eat_sym(',') {
                        break;
                    }
                }
            } else if self.eat_kw("REMOVE") {
                if std::mem::replace(&mut seen_remove, true) {
                    return Err(dup("REMOVE", self.what));
                }
                loop {
                    u.remove.push(self.path()?);
                    if !self.eat_sym(',') {
                        break;
                    }
                }
            } else if self.at_kw("ADD") || self.at_kw("DELETE") {
                let is_add = self.at_kw("ADD");
                self.bump();
                if std::mem::replace(
                    if is_add {
                        &mut seen_add
                    } else {
                        &mut seen_delete
                    },
                    true,
                ) {
                    return Err(dup(if is_add { "ADD" } else { "DELETE" }, self.what));
                }
                loop {
                    let p = self.path()?;
                    let v = self.operand()?;
                    if is_add {
                        u.add.push((p, v))
                    } else {
                        u.delete.push((p, v))
                    }
                    if !self.eat_sym(',') {
                        break;
                    }
                }
            } else {
                return Err(self.err());
            }
        }
        if u.set.is_empty() && u.remove.is_empty() && u.add.is_empty() && u.delete.is_empty() {
            return Err(format!(
                "Invalid {}: The expression can not be empty;",
                self.what
            ));
        }
        Ok(u)
    }

    fn finish<T>(&self, v: T) -> Result<T, String> {
        if *self.peek() == Tok::End {
            Ok(v)
        } else {
            Err(self.err())
        }
    }
}

pub fn parse_condition(src: &str, what: &'static str, env: &mut Env) -> Result<Cond, String> {
    let mut p = Parser::new(src, what, env)?;
    let c = p.or()?;
    p.finish(c)
}

pub fn parse_update(src: &str, env: &mut Env) -> Result<UpdateExpr, String> {
    let mut p = Parser::new(src, "UpdateExpression", env)?;
    let u = p.update()?;
    p.finish(u)
}

pub fn parse_projection(src: &str, env: &mut Env) -> Result<Vec<Path>, String> {
    let mut p = Parser::new(src, "ProjectionExpression", env)?;
    let mut paths = vec![p.path()?];
    while p.eat_sym(',') {
        paths.push(p.path()?);
    }
    p.finish(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env_with<'a>(
        names: &'a BTreeMap<String, String>,
        values: &'a Map<String, Value>,
    ) -> Env<'a> {
        Env::new(Some(names), Some(values))
    }

    fn vals(keys: &[&str]) -> Map<String, Value> {
        keys.iter()
            .map(|k| (k.to_string(), json!({"N": "1"})))
            .collect()
    }

    #[test]
    fn parses_conditions_with_precedence() {
        let v = vals(&[":a", ":b", ":c"]);
        let mut env = Env::new(None, Some(&v));
        let c = parse_condition(
            "x = :a OR y > :b AND NOT z <= :c",
            "ConditionExpression",
            &mut env,
        )
        .unwrap();
        assert!(matches!(c, Cond::Or(_, _)));
        if let Cond::Or(_, right) = c {
            assert!(matches!(*right, Cond::And(_, _)));
        }
        assert!(env.unused_error().is_none());
    }

    #[test]
    fn parses_functions_between_in_and_paths() {
        let v = vals(&[":lo", ":hi", ":p"]);
        let mut env = Env::new(None, Some(&v));
        let c = parse_condition(
            "attribute_exists(a.b[2].c) AND age BETWEEN :lo AND :hi AND begins_with(nm, :p) AND size(tags) > :lo AND k IN (:lo, :hi)",
            "FilterExpression",
            &mut env,
        )
        .unwrap();
        assert!(matches!(c, Cond::And(_, _)));
    }

    #[test]
    fn name_and_value_resolution_errors() {
        let mut env = Env::new(None, None);
        let e = parse_condition("#a = :b", "ConditionExpression", &mut env).unwrap_err();
        assert!(e.contains("An expression attribute name used in the document path is not defined; attribute name: #a"), "{e}");

        let names = BTreeMap::from([("#a".to_string(), "real".to_string())]);
        let v = vals(&[]);
        let mut env = env_with(&names, &v);
        let e = parse_condition("#a = :b", "ConditionExpression", &mut env).unwrap_err();
        assert!(
            e.contains("attribute value used in expression is not defined; attribute value: :b"),
            "{e}"
        );
    }

    #[test]
    fn reserved_words_and_unused() {
        let v = vals(&[":v", ":unused"]);
        let mut env = Env::new(None, Some(&v));
        let e = parse_condition("name = :v", "ConditionExpression", &mut env).unwrap_err();
        assert!(e.contains("reserved keyword: name"), "{e}");
        let mut env = Env::new(None, Some(&v));
        parse_condition("foo = :v", "ConditionExpression", &mut env).unwrap();
        assert_eq!(
            env.unused_error().unwrap(),
            "Value provided in ExpressionAttributeValues unused in expressions: keys: {:unused}"
        );
    }

    #[test]
    fn parses_updates() {
        let v = vals(&[":one", ":s", ":l"]);
        let mut env = Env::new(None, Some(&v));
        let u = parse_update(
            "SET a = :one, b = a + :one, c = list_append(c, :l), d = if_not_exists(d, :one) REMOVE e, f[1] ADD g :one DELETE h :s",
            &mut env,
        )
        .unwrap();
        assert_eq!(u.set.len(), 4);
        assert_eq!(u.remove.len(), 2);
        assert_eq!(u.add.len(), 1);
        assert_eq!(u.delete.len(), 1);
        assert!(matches!(u.set[1].1, SetValue::Plus(_, _)));
    }

    #[test]
    fn update_errors() {
        let v = vals(&[":one"]);
        let mut env = Env::new(None, Some(&v));
        let e = parse_update("SET a = :one SET b = :one", &mut env).unwrap_err();
        assert!(
            e.contains("The \"SET\" section can only be used once"),
            "{e}"
        );
        let mut env = Env::new(None, Some(&v));
        assert!(parse_update("", &mut env).is_err());
        let mut env = Env::new(None, Some(&v));
        assert!(
            parse_update("SET a :one", &mut env)
                .unwrap_err()
                .contains("Syntax error")
        );
    }

    #[test]
    fn parses_projection() {
        let names = BTreeMap::from([("#n".to_string(), "name".to_string())]);
        let mut env = Env::new(Some(&names), None);
        let p = parse_projection("id, #n, a.b[0]", &mut env).unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!(p[1], vec![PathElem::Attr("name".into())]);
        assert_eq!(
            p[2],
            vec![
                PathElem::Attr("a".into()),
                PathElem::Attr("b".into()),
                PathElem::Index(0)
            ]
        );
    }
}
