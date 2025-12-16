
def cmd_db_test(args):
    db = args.ctx.db
    if db.test() is True:
        print("✅ MySQL client test passed.")
    else:
        print("❌ MySQL client test failed.")