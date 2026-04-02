# Color and Theme Architecture

Internal reference for the color/theme system in `src/gza/colors.py`.

## Module structure

Everything lives in `src/gza/colors.py`. There are no color definitions elsewhere in the codebase — a test (`test_no_hex_colors_outside_colors_module`) enforces this.

### Base palette

Module-level constants with semantic names (`pink`, `gray_secondary`, `blue_neon`, etc.) mapped to Rich color strings. These are the building blocks referenced by dataclass defaults and theme overrides.

### Domain color dataclasses

Seven frozen dataclasses, one per output context:

| Class | Used by |
|-------|---------|
| `TaskColors` | `gza history`, `gza stats` |
| `StatusColors` | `gza ps`, lineage trees |
| `WorkOutputColors` | Live provider stream output |
| `ShowColors` | `gza show` |
| `UnmergedColors` | `gza unmerged` |
| `LineageColors` | `gza lineage` |
| `NextColors` | `gza next` |

Each field has a hardcoded default (lowest priority).

### BaseColors

Cross-cutting defaults for fields that appear in multiple domain classes (`task_id`, `prompt`, `stats`, `branch`, `label`, `value`, `heading`). A theme's `base` dict overrides these fields across all domain classes simultaneously.

### Theme model

`Theme` is a frozen dataclass with a `name`, a `base` dict, and one dict per domain class. Resolution priority (highest wins):

1. Ad-hoc `color_overrides` from config
2. Per-domain dict (e.g. `theme.task`)
3. `base` dict (only for `BaseColors` fields)
4. Dataclass field defaults

#### Theme.uniform()

`Theme.uniform(name, color)` creates a theme that sets every field in every domain class to a single color. Uses the `_all_fields(color, cls)` helper. Useful for monochrome themes or as a base for themes with a few exceptions:

```python
# All pink, one line:
_THEME_PINK = Theme.uniform("pink", pink)

# Mostly gray, with white accents:
_dd = Theme.uniform("default_dark", gray_light1)
_THEME_DEFAULT_DARK = dataclasses.replace(
    _dd,
    base={**_dd.base, "value": "white", "heading": "white"},
    ...
)
```

### Built-in themes

Registered in `BUILT_IN_THEMES` dict, keyed by name. Config validation rejects unknown theme names.

### Theme application

`set_theme(theme_name, color_overrides)` is called by `Config.load()` at startup. It rebuilds all module-level singletons (`TASK_COLORS`, `TASK_COLORS_DICT`, etc.) via `_build_themed_instances()` → `_apply_domain_theme()`.

Code imports singletons as `import gza.colors as c` and accesses `c.TASK_COLORS.field`. The `_DICT` variants exist for backward compatibility with dict-style access.

### Excluded from theming

These dicts use hardcoded semantic colors (green/red/yellow) and are intentionally not affected by `set_theme()`:

- `LOG_TASK_STATUS_COLORS`
- `LOG_WORKER_STATUS_COLORS`
- `REVIEW_VERDICT_COLORS`
- `CYCLE_STATUS_COLORS`

## Adding a new theme

1. Define the theme in `src/gza/colors.py` using `Theme(...)` or `Theme.uniform(...)`.
2. Add it to the `BUILT_IN_THEMES` list.
3. Add tests in `tests/test_colors.py`.
4. Users set `theme: <name>` in `gza.yaml`.

## Adding a new color field

1. Add the field to the appropriate domain dataclass.
2. If it should be cross-cutting, also add it to `BaseColors`.
3. Update any existing themes that should override it.
4. Update the key-set regression test for that dataclass.
