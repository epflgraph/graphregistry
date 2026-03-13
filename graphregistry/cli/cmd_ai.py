
#-------------------------------#
# Handler: Test GraphAI service #
#-------------------------------#
def cmd_ai_test(args):
    """
    Handle:
      graphregistry ai test [...]
    """

    # Fetch context objects
    ai = args.ctx.ai
    graphai_auth_token = args.ctx.graphai_auth_token

    # Execute command:
    # - Test connection to GraphAI server

    # Translate "Hello, world!" from English to French
    in_text = "Hello, world!"
    out_text = ai.client_api.translation.translate_text_str(text=in_text, source_language='en', target_language='fr', force=True, login_info=graphai_auth_token).strip()

    # Print translation result
    print(f"""🌐 Translation test result: "{in_text}" [en] --> "{out_text}" [fr]""")

    # Print test results
    if len(out_text) > 4:
        print(f"✅ GraphAI server is up and running.")
    else:
        print(f"❌ GraphAI server is down or unreachable.")

    # Print footers
    print("🖥️  ~ Done.")
