import os

_pw = os.getenv("POSTGRES_PASSWORD", "change_me_primary")
PRIMARY_DSN = os.getenv("PRIMARY_DSN", f"postgresql://postgres:{_pw}@localhost:5433/pulsestream")
REPLICA_DSNS = os.getenv("REPLICA_DSNS", f"postgresql://postgres:{_pw}@localhost:5434/pulsestream").split(",")