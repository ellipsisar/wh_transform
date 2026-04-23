# Prompt — Actualizar CLAUDE.md del proyecto wh_transform

> Copiar y pegar este prompt completo en Claude Code cada vez que:
> - Se agregan nuevas fuentes de datos
> - Se crean modelos nuevos en staging/intermediate/marts
> - Cambian las convenciones del proyecto
> - Se instalan nuevos paquetes DBT
> - Cambia el data warehouse o adapter

---

## Prompt para pegar en Claude Code

```
Necesito que actualices el archivo .claude/CLAUDE.md para que refleje el estado real actual
del proyecto wh_transform.

Por favor seguí estos pasos en orden:

PASO 1 — Leer la configuración del proyecto
- Lee dbt_project.yml completo
- Lee packages.yml completo
- Lee profiles.yml si existe (para identificar el adapter/warehouse)

PASO 2 — Mapear la estructura de modelos
- Usa Glob para listar todos los archivos: models/**/*.sql
- Usa Glob para listar todos los YAMLs: models/**/*.yml
- Identifica qué capas existen (staging, intermediate, marts, u otras)
- Identifica los dominios presentes (por nombre de carpeta o prefijo de modelo)

PASO 3 — Leer las fuentes registradas
- Lee todos los archivos sources.yml que encuentres
- Extrae: nombre del source, schema, tablas registradas

PASO 4 — Leer los modelos de marts (los más importantes para BI)
- Lee los modelos en marts/ para entender su grain y propósito
- Si tienen description en schema.yml, usarla directamente

PASO 5 — Actualizar CLAUDE.md
Con toda la información recopilada, actualizá ÚNICAMENTE estas secciones del archivo
.claude/CLAUDE.md (no toques el resto):

a) En el header del proyecto: completar adapter DBT y warehouse destino si los encontraste
b) "Estructura del Proyecto": reemplazar el árbol de directorios con la estructura real,
   listando las carpetas existentes y ejemplos de modelos reales (no inventar)
c) "Fuentes de Datos Registradas": completar la tabla con los sources encontrados
d) "Modelos Clave del Proyecto": completar con los modelos de marts (y los de staging
   más importantes), su layer, dominio, grain si está documentado, y si tienen tests
e) "Packages Instalados": copiar el contenido real de packages.yml
f) Si encontrás convenciones de naming que difieren de las documentadas (ej: usan
   prefijos distintos, tienen carpetas extra), agregá una nota en "Convenciones de Naming"

Reglas estrictas:
- NO modificar: Agent Team, Routing Rules, Coding Standards, reglas de Git
- NO inventar modelos o fuentes que no existan en los archivos
- NO eliminar los 📌 COMPLETAR que no puedas resolver con los archivos disponibles
- Si un archivo no existe o está vacío, indicarlo con "(archivo no encontrado)" en la sección
- Guardar el resultado en .claude/CLAUDE.md
- Al finalizar, mostrar un resumen de qué secciones se actualizaron y cuáles quedaron pendientes
```

---

## Cuándo correr este prompt

| Evento | Acción |
|--------|--------|
| Primera configuración del proyecto | Correr una vez para poblar todo |
| Se agrega una nueva fuente (`sources.yml`) | Correr para actualizar "Fuentes de Datos" |
| Se crean modelos nuevos en marts/ | Correr para actualizar "Modelos Clave" |
| Se instala un nuevo paquete en packages.yml | Correr para actualizar "Packages" |
| Refactor que cambia estructura de carpetas | Correr para actualizar "Estructura" |
| Cambio de adapter o warehouse | Correr para actualizar header del proyecto |

---

## Verificación post-actualización

Después de correr el prompt, verificar que:
- [ ] El adapter DBT está indicado (bigquery, snowflake, synapse, etc.)
- [ ] La tabla de fuentes tiene al menos una entrada real
- [ ] La tabla de modelos clave tiene al menos los marts principales
- [ ] La estructura de directorios refleja lo que hay en `models/`
- [ ] No se eliminaron las secciones de Agent Team o Routing Rules
