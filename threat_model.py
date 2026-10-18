"""Threat model for the Lab Inventory demo app, written with pytm.

pytm is a code-defined threat modeling tool: describe the system as elements and
data flows, and it applies a library of STRIDE-style threats to produce a report.
This is the scriptable alternative to the Microsoft Threat Modeling Tool shown in
the course materials.

    pip install pytm
    python threat_model.py --list                 # all threats pytm knows about
    python threat_model.py --report tm_template.md > threat_report.md  # requires an existing pytm template
    python threat_model.py --dfd | dot -Tpng -o dfd.png    # needs Graphviz
"""

from pytm import Actor, Boundary, Data, Dataflow, Datastore, Process, TM

# Initialize Threat Model
tm = TM("Lab Inventory CLI")
tm.description = (
    "A single-user command-line inventory tool. Admin actions are gated by a "
    "passphrase whose salted hash lives in the OS credential store."
)
tm.isOrdered = True

# Trust boundaries
user_zone = Boundary("User session", description="Everything running as the logged-on user")
os_zone = Boundary("Operating system", description="Protected storage managed by Windows")

# Elements
user = Actor(
    "Lab user",
    description="Person at the keyboard; may or may not know the admin passphrase",
)
user.inBoundary = user_zone

app = Process("lab_inventory.py", description="The CLI application")
app.inBoundary = user_zone
app.controls.implementsAuthenticationScheme = True
app.controls.hasAccessControl = True
app.controls.validatesInput = True
app.controls.sanitizesInput = True
app.handlesResources = True
app.controls.checksInputBounds = True

# Datastores
cred_store = Datastore(
    "OS Credential Store",
    description="Windows Credential Manager / OS vault accessed via keyring",
)
cred_store.inBoundary = os_zone
cred_store.isShared = False
cred_store.storesSensitiveData = True

inventory_db = Datastore(
    "inventory.json",
    description="Local JSON flat file containing lab inventory records",
)
inventory_db.inBoundary = user_zone
inventory_db.isShared = False

log_file = Datastore(
    "lab-inventory.log",
    description="Local application log file recording operational events",
)
log_file.inBoundary = user_zone
log_file.isShared = False

# Data definitions
user_input_data = Data(
    "User Commands & Credentials",
    description="CLI arguments, options, and admin passphrase input",
    isCredentials=True,
)
admin_hash_data = Data(
    "Admin Password Hash",
    description="Salted hash retrieved for verification",
    isPII=False,
    isCredentials=True,
    isStored=True,
)
inventory_records = Data(
    "Inventory Data",
    description="JSON records of lab assets and equipment",
    isStored=True,
)
log_events = Data(
    "Application Logs",
    description="Audit logs, operational messages, and error traces",
    isStored=True,
)

# Dataflows
df_input = Dataflow(user, app, "Inputs commands and passphrase")
df_input.data = user_input_data

df_output = Dataflow(app, user, "Displays inventory output / status")
df_output.data = inventory_records

df_cred_read = Dataflow(cred_store, app, "Retrieves stored admin hash for authentication")
df_cred_read.data = admin_hash_data

df_cred_write = Dataflow(app, cred_store, "Stores initial/updated admin credential hash")
df_cred_write.data = admin_hash_data

df_inv_read = Dataflow(inventory_db, app, "Reads inventory records from JSON file")
df_inv_read.data = inventory_records

df_inv_write = Dataflow(app, inventory_db, "Persists inventory updates to JSON file")
df_inv_write.data = inventory_records

df_log_write = Dataflow(app, log_file, "Appends activity events and audit logs")
df_log_write.data = log_events

# Execute pytm analysis engine and process its command-line options.
if __name__ == "__main__":
    tm.process()
