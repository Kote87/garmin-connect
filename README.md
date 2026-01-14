# garmin-connect

## Actualización diaria de datos (catch-up + diario)

Ejemplo de ejecución manual:

```bash
python scripts/update_recent.py --end yesterday --chunk-days 7
```

Cron (simple):

```bash
crontab -e
```

Añadir:

```bash
15 2 * * * cd /home/kote78/Proyectos/garmin-connect && /home/kote78/Proyectos/garmin-connect/.venv/bin/python scripts/update_recent.py --end yesterday --chunk-days 7 >> logs/update_recent.log 2>&1
```
