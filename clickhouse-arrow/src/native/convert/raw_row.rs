use std::borrow::Cow;

use crate::{Error, FromSql, Result, Row, ToSql, Type, Value};

/// A row of raw data returned from the database by a query.
/// Or an unstructured runtime-defined row to upload to the server.
#[derive(Debug, Default, Clone)]
pub struct RawRow(Vec<Option<(String, Type, Value)>>);

impl Row for RawRow {
    const COLUMN_COUNT: Option<usize> = None;

    fn column_names() -> Option<Vec<Cow<'static, str>>> { None }

    fn to_schema() -> Option<Vec<(String, Type, Option<Value>)>> { None }

    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        Ok(Self(
            map.into_iter()
                .map(|(name, type_, value)| Some((name.to_string(), type_.clone(), value)))
                .collect(),
        ))
    }

    fn serialize_row(
        self,
        _type_hints: &[(String, Type)],
    ) -> Result<Vec<(Cow<'static, str>, Value)>> {
        Ok(self
            .0
            .into_iter()
            .map(|x| x.expect("cannot serialize a Row which has been retrieved from"))
            .map(|(name, _, value)| (Cow::Owned(name), value))
            .collect())
    }
}

pub trait RowIndex {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize>;
}

impl RowIndex for usize {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize> {
        let count = columns.into_iter().count();
        if count >= *self { Some(*self) } else { None }
    }
}

impl RowIndex for str {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize> {
        columns.into_iter().position(|x| x == self)
    }
}

impl<T: RowIndex + ?Sized> RowIndex for &T {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize> {
        (*self).get(columns)
    }
}

impl RawRow {
    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize { self.0.len() }

    /// # Panics
    ///
    /// Panics if any of the values are `None`
    pub fn into_values(self) -> Vec<(Type, Value)> {
        self.0.into_iter().map(|x| x.map(|(_, t, v)| (t, v)).unwrap()).collect()
    }

    /// Like [`RawRow::get`], but returns a [`Result`] rather than panicking.
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds or if the value cannot be converted to the
    /// specified
    ///
    /// # Panics
    ///
    /// Shouldn't panic, bounds checked
    pub fn try_get<I: RowIndex, T: FromSql>(&mut self, index: I) -> Result<T> {
        let index = index
            .get(self.0.iter().map(|x| x.as_ref().map_or("", |x| &*x.0)))
            .ok_or(Error::OutOfBounds)?;
        let (_, type_, value) = self.0.get_mut(index).unwrap().take().ok_or(Error::DoubleFetch)?;
        T::from_sql(&type_, value)
    }

    /// Deserializes a value from the row.
    /// The value can be specified either by its numeric index in the row, or by its column name.
    /// # Panics
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified
    /// type.
    pub fn get<I: RowIndex, T: FromSql>(&mut self, index: I) -> T {
        self.try_get(index).expect("failed to convert column")
    }

    /// Sets or inserts a column value with a given name. `type_` is inferred if `None`. Index is
    /// defined on insertion order.
    ///
    /// # Errors
    ///
    /// Returns an error if type conversion fails.
    ///
    /// # Panics
    ///
    /// Shouldn't panic as `current_position` checks for existence of element
    pub fn try_set_typed(
        &mut self,
        name: &impl ToString,
        type_: Option<Type>,
        value: impl ToSql,
    ) -> Result<()> {
        let name = name.to_string();
        let value = value.to_sql(type_.as_ref())?;
        let type_ = type_.unwrap_or_else(|| value.guess_type());

        let current_position =
            self.0.iter().map(|x| x.as_ref().map_or("", |x| &*x.0)).position(|x| x == &*name);

        if let Some(current_position) = current_position {
            self.0[current_position].as_mut().unwrap().1 = type_;
            self.0[current_position].as_mut().unwrap().2 = value;
        } else {
            self.0.push(Some((name, type_, value)));
        }
        Ok(())
    }

    /// Same as `try_set_typed`, but always infers the type
    ///
    /// # Errors
    ///
    /// Returns an error if type conversion fails.
    pub fn try_set(&mut self, name: &impl ToString, value: impl ToSql) -> Result<()> {
        self.try_set_typed(name, None, value)
    }

    /// Same as `try_set`, but panics on type conversion failure.
    ///
    /// # Panics
    ///
    /// Panics on type conversion failure
    pub fn set(&mut self, name: &impl ToString, value: impl ToSql) {
        self.try_set(name, value).expect("failed to convert column");
    }

    /// Same as `try_set_typed`, but panics on type conversion failure.
    /// # Panics
    ///
    /// Panics on type conversion failure
    pub fn set_typed(&mut self, name: &impl ToString, type_: Option<Type>, value: impl ToSql) {
        self.try_set_typed(name, type_, value).expect("failed to convert column");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_row_default() {
        let row = RawRow::default();
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);
    }

    #[test]
    fn test_raw_row_basic_operations() {
        let mut row = RawRow::default();

        // Test empty row
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);

        // Test setting values
        row.set(&"test_col", 42i32);
        assert!(!row.is_empty());
        assert_eq!(row.len(), 1);

        // Test try_set
        row.try_set(&"test_col2", "hello").unwrap();
        assert_eq!(row.len(), 2);
    }

    #[test]
    fn test_raw_row_set_typed() {
        let mut row = RawRow::default();

        // Test with explicit type
        row.set_typed(&"int_col", Some(Type::Int32), 123i32);
        assert_eq!(row.len(), 1);

        // Test try_set_typed
        row.try_set_typed(&"str_col", Some(Type::String), "test").unwrap();
        assert_eq!(row.len(), 2);

        // Test updating existing column
        row.try_set_typed(&"int_col", Some(Type::Int32), 456i32).unwrap();
        assert_eq!(row.len(), 2); // Should still be 2, not 3
    }

    #[test]
    fn test_raw_row_get_by_index() {
        let mut row = RawRow::default();
        row.set(&"col1", 42i32);
        row.set(&"col2", "hello");

        // Test getting by numeric index
        let val: i32 = row.try_get(0usize).unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn test_raw_row_get_by_name() {
        let mut row = RawRow::default();
        row.set(&"test_column", 123i64);

        // Test getting by column name
        let val: i64 = row.try_get("test_column").unwrap();
        assert_eq!(val, 123);
    }

    #[test]
    fn test_raw_row_get_panics() {
        let mut row = RawRow::default();
        row.set(&"test", 42i32);

        // This should work
        let _val: i32 = row.get(0usize);

        // Second get should panic due to double fetch
        drop(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _val2: i32 = row.get(0usize);
            }))
            .unwrap_err(),
        );
    }

    #[test]
    fn test_raw_row_errors() {
        let mut row = RawRow::default();
        row.set(&"test", 42i32);

        // Test out of bounds error
        let result: Result<i32> = row.try_get(10usize);
        assert!(matches!(result, Err(Error::OutOfBounds)));

        // Test double fetch error
        let _val: i32 = row.try_get(0usize).unwrap();
        let result2: Result<i32> = row.try_get(0usize);
        assert!(matches!(result2, Err(Error::DoubleFetch)));
    }

    #[test]
    fn test_raw_row_into_values() {
        let mut row = RawRow::default();
        row.set(&"col1", 42i32);
        row.set(&"col2", "test");

        let values = row.into_values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].0, Type::Int32);
        assert_eq!(values[1].0, Type::String);
    }

    #[test]
    fn test_row_index_usize() {
        let columns = ["col1", "col2", "col3"];

        // Valid index
        assert_eq!(1usize.get(columns.iter().copied()), Some(1));

        // Invalid index
        assert_eq!(5usize.get(columns.iter().copied()), None);
    }

    #[test]
    fn test_row_index_str() {
        let columns = ["col1", "col2", "col3"];

        // Valid column name
        assert_eq!(RowIndex::get("col2", columns.iter().copied()), Some(1));

        // Invalid column name
        assert_eq!(RowIndex::get("nonexistent", columns.iter().copied()), None);
    }

    #[test]
    fn test_row_index_ref() {
        let columns = ["col1", "col2", "col3"];
        let name = "col2";

        // Test reference implementation
        assert_eq!((&name).get(columns.iter().copied()), Some(1));
        assert_eq!((1usize).get(columns.iter().copied()), Some(1));
    }

    #[test]
    fn test_raw_row_deserialize() {
        let map = vec![
            ("col1", &Type::Int32, Value::Int32(42)),
            ("col2", &Type::String, Value::String("test".to_string().into_bytes())),
        ];

        let row = RawRow::deserialize_row(map).unwrap();
        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_raw_row_serialize() {
        let mut row = RawRow::default();
        row.set(&"col1", 42i32);
        row.set(&"col2", "test");

        let serialized = row.serialize_row(&[]).unwrap();
        assert_eq!(serialized.len(), 2);
        assert_eq!(serialized[0].0, "col1");
        assert_eq!(serialized[1].0, "col2");
    }

    #[test]
    fn test_raw_row_serialize_panic() {
        // Create a row with a None value (which should cause serialize to panic)
        let raw_row = RawRow(vec![None]);

        // This should panic because we have a None value
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            raw_row.serialize_row(&[]).unwrap()
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_raw_row_static_methods() {
        // Test Row trait static methods
        assert_eq!(RawRow::COLUMN_COUNT, None);
        assert_eq!(RawRow::column_names(), None);
        assert_eq!(RawRow::to_schema(), None);
    }
}
