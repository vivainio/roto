use crate::timestamp::Timestamp;

/// Minimal streaming XML writer (elements and escaped text only; no pretty-printing, because
/// AWS responses are compact and some moto tests assert there are no newlines).
#[derive(Default)]
pub struct XmlWriter {
    buf: String,
}

impl XmlWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self, name: &str) {
        self.open_raw(name, &[]);
    }

    pub(crate) fn open_raw(&mut self, name: &str, attrs: &[(&str, &str)]) {
        self.buf.push('<');
        self.buf.push_str(name);
        for (k, v) in attrs {
            self.buf.push(' ');
            self.buf.push_str(k);
            self.buf.push_str("=\"");
            escape_into(&mut self.buf, v);
            self.buf.push('"');
        }
        self.buf.push('>');
    }

    pub fn close(&mut self, name: &str) {
        self.buf.push_str("</");
        self.buf.push_str(name);
        self.buf.push('>');
    }

    pub fn text(&mut self, s: &str) {
        escape_into(&mut self.buf, s);
    }

    pub fn element(&mut self, name: &str, text: &str) {
        self.open(name);
        self.text(text);
        self.close(name);
    }

    pub fn finish(self) -> String {
        self.buf
    }
}

fn escape_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
}

/// A value that can be written as `<name>…</name>`.
pub trait XmlValue {
    fn write(&self, w: &mut XmlWriter, name: &str);
}

impl XmlValue for String {
    fn write(&self, w: &mut XmlWriter, name: &str) {
        w.element(name, self);
    }
}

macro_rules! display_xml {
    ($($t:ty),*) => {$(
        impl XmlValue for $t {
            fn write(&self, w: &mut XmlWriter, name: &str) { w.element(name, &self.to_string()); }
        }
    )*};
}
display_xml!(i32, i64, bool, f64);

impl XmlValue for crate::json::Blob {
    fn write(&self, w: &mut XmlWriter, name: &str) {
        w.element(name, &crate::base64::encode(&self.0));
    }
}

impl XmlValue for Timestamp {
    fn write(&self, w: &mut XmlWriter, name: &str) {
        w.element(name, &self.to_iso8601());
    }
}

impl XmlWriter {
    /// Writes a map as `<name><entry><key/><value/></entry>…</name>` (or repeated `<name>` when flattened).
    pub fn map<V: XmlValue>(
        &mut self,
        name: &str,
        flattened: bool,
        key_name: &str,
        value_name: &str,
        entries: &std::collections::BTreeMap<String, V>,
    ) {
        if !flattened {
            self.open(name);
        }
        for (k, v) in entries {
            let entry = if flattened { name } else { "entry" };
            self.open(entry);
            self.element(key_name, k);
            v.write(self, value_name);
            self.close(entry);
        }
        if !flattened {
            self.close(name);
        }
    }

    /// Writes a list. Flattened lists repeat `name`; otherwise items sit in `<name><item/>…</name>`.
    pub fn list<T: XmlValue>(&mut self, name: &str, item: &str, flattened: bool, items: &[T]) {
        if flattened {
            items.iter().for_each(|i| i.write(self, name));
        } else {
            self.open(name);
            items.iter().for_each(|i| i.write(self, item));
            self.close(name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_text() {
        let mut w = XmlWriter::new();
        "a<b>&\"'".to_string().write(&mut w, "X");
        assert_eq!(w.finish(), "<X>a&lt;b&gt;&amp;&quot;&apos;</X>");
    }

    #[test]
    fn writes_lists() {
        let mut w = XmlWriter::new();
        w.list("L", "member", false, &[1i32, 2]);
        w.list("F", "member", true, &[3i32]);
        assert_eq!(
            w.finish(),
            "<L><member>1</member><member>2</member></L><F>3</F>"
        );
    }
}
