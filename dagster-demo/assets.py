import dagster as dg

@dg.asset
def hello(context: dg.AssetExecutionContext):
    context.log.info("Hello, world!")

@dg.asset(deps=[hello])
def hello_again(context: dg.AssetExecutionContext):
    context.log.info("Hello, world again!")


defs = dg.Definitions(assets=[hello, hello_again])