# Simple ORM for Spring JDBC (sorm4sj)

Простой ORM предназначен для связи классов Java с таблицами базы данных и типов данных Java с типами данных SQL в среде Spring JDBC.

Не предоставляет возможностей для создания таблиц/триггеров и прочих сущностей СУБД, а также не генерирует SQL-запросы.

Требует наличия spring-jdbc в зависимостях проекта, в котором используется этот ORM.

Для работы с СУБД предоставляется класс ORMRepository

* для получения простых данных или выполнения DDL
* для маппинга результата в объекты Java

## Примеры использования ORMRepository
### Инициализация
```java
@Repository
class SomeRepository extends ORMRepository {
    
    @Autowired
    public SomeRepository( NamedParameterJdbcTemplate jdbcTemplate ) {
        super( jdbcTemplate );
    }
    
}
```

### Получение скаляров
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public int getCount() {
        return super.loadScalar( Integer.class, "SELECT COUNT(*) FROM table" );
    }
    
} 
```

### Получение строки без маппинга
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public Map<String, Object> getObject() {
        return super.loadObject( "SELECT a, b, c FROM table LIMIT 1" );
    }
    
} 
```

### Получение одной колонки
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public String getColumn() {
        return super.loadColumn( String.class, "SELECT a FROM table" );
    }
    
} 
```

### Получение всех строк без маппинга
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public Iterable<Map<String, Object>> getList() {
        return super.loadObjects( "SELECT a, b, c FROM table" );
    }
    
} 
```

### Выполнение запроса без возвращаемых результатов
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public void update() {
        return super.execute( "UPDATE table SET a = 1 WHERE b = 2" );
    }
    
} 
```

### Использование неименованных параметров запроса
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public int getCount( int id, List<String> names ) {
        return super.loadScalar( 
            Integer.class, 
            "SELECT COUNT(*) FROM table WHERE id = ? AND name = ANY( ? )",
            id, names 
        );
    }
    
} 
```

### Использование именованных параметров запроса
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public int getCount( int id, String name ) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue( "id", id );
        params.addValue( "name", name );
        return super.loadScalar( 
            Integer.class, 
            "SELECT COUNT(*) FROM table WHERE id = :id AND name = :name",
            id, name
        );
    }
    
} 
```

### Получение строки с маппингом
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public SomeClass loadItem() {
        return super.loadObject( SomeClass.class, "SELECT a, b, c FROM table LIMIT 1" );
    }
    
} 
```

### Получение строк с маппингом
```java
@Repository
class SomeRepository extends ORMRepository {
    
    public Iterable<SomeClass> loadItem() {
        return super.loadList( SomeClass.class, "SELECT a, b, c FROM table" );
    }
    
} 
```

## Описание объектов для маппинга

### Маппинг полей
```java
@Entity
class SomeClass {
    
    @Column( name = "a" )
    private int a;
    
    @Column( name = "b" )
    private int b;
    
    @Column( name = "c" )
    private int c;
    
}
```

Для неабстрактных классов-родителей требуется использование @MappedSuperclass
```java
@MappedSuperclass
class SomeClassParent {
    
        @Column( name = "a" )
        private int a;
        
        @Column( name = "b" )
        private int b;
        
        @Column( name = "c" )
        private int c;
        
}

@Entity
class SomeClass extends SomeClass {
    
    @Column( name = "d" )
    private int d;
    
}
```

### Маппинг методов
```java
@Entity
class SomeClass {
    
    @Column( name = "a" )
    private int a;
    
    @Column( name = "b" )
    private int b;
    
    @Column( name = "c" )
    private int c;
    
    @Column( name = "ids" )
    private void setIds( String ids ) {
        // логика преобразования строки ids
    }
    
}
```
**Примечание** - метод, помеченный @Column требует наличия одного аргумента простого типа (String, Integer и т. д.)