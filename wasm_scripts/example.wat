(module
  ;; Экспорт памяти
  (memory (export "memory") 10)
  
  ;; Экспорт функций управления памятью
  (func (export "malloc") (param i32) (result i32)
    ;; Выделение памяти
    ...
  )
  
  (func (export "free") (param i32)
    ;; Освобождение памяти
    ...
  )
  
  ;; Трансформация данных
  (func (export "transform") (param i32 i32) (result i32)
    ;; Чтение входных данных
    ;; Трансформация
    ;; Запись результата: [длина:4][данные...]
    ;; Возврат указателя на результат
    ...
  )
  
  ;; Маскирование значения
  (func (export "mask_value") (param i32 i32 i32 i32) (result i32)
    ;; colNamePtr, colNameLen, valuePtr, valueLen
    ;; Маскирование значения
    ;; Запись результата: [длина:4][данные...]
    ;; Возврат указателя на результат
    ...
  )
)