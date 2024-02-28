import great_expectations as gx

context = gx.get_context()  # gets a great expectations project context
ds = context.sources.add_postgres(
    name="glaredb",
    connection_string="postgresql://sean:sean@localhost:6543/db",
)
