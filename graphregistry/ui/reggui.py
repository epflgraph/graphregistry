#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from loguru import logger as sysmsg
from collections import defaultdict
from tkinter import ttk
import tkinter as tk
import rich

#=======================================================#
# Function definition: Tkinter Graphical User Interface #
#=======================================================#
def LaunchGUI(gr):

    # Initialize the main window
    root = tk.Tk()
    root.title("GraphRegistry GUI")
    root.geometry("1020x1020")

    # Configure grid weights for resizing behavior
    root.grid_rowconfigure(0, weight=1)     # Allow row 0 to expand vertically
    root.grid_columnconfigure(0, weight=0)  # Column 0 doesn't need to stretch horizontally
    root.grid_columnconfigure(1, weight=0)

    # Node types
    list_of_node_types = ['Category', 'Concept', 'Course', 'Lecture', 'MOOC', 'Person', 'Publication', 'Startup', 'Unit', 'Widget']

    # Initialise GUI elements dict
    gui = {
        'orchestration' : {
            'subframe' : None,
            'description' : None,
            'node_types' : {
                'subframe' : None,
                'description' : None,
                'list' : [],
                'button_add_new' : None,
            },
            'edge_types' : {
                'subframe' : None,
                'description' : None,
                'list' : [],
                'button_add_new' : None,
            },
        },
        'processing' : {
            'subframe' : None,
            'description' : None,
            'cachemanage' : {
                'subframe' : None,
                'description' : None,
                'actions' : {'print':False, 'eval':False, 'commit':False},
            },
            'indexdb' : {
                'subframe' : None,
                'description' : None,
                'actions' : {'print':False, 'eval':False, 'commit':False},
                'engine' : False,
                'cache_buildup' : {'subframe' : None},
                'page_profile' : {'subframe' : None},
                'index_docs' : {'subframe' : None},
                'index_doc_links' : {'subframe' : None},
                'index_docs_es' : {'subframe' : None},
                'index_doc_links_es' : {'subframe' : None},
            },
            'elasticsearch' : {
                'subframe' : None,
                'description' : None,
                'actions' : {'print':False, 'eval':False, 'commit':False},
            },
        }
    }

    #================================================#
    # Functions handling button creation and actions #
    #================================================#
    if True:

        #--------------------------------------------#
        checkbox_rows_stack = []
        def create_action_checkboxes_row(frame_pointer, var_pointer, include_engine=False):

            # Create now checkbox row in stack
            checkbox_row = tk.Frame(frame_pointer)
            checkbox_row.pack(anchor='w', expand=False)

            # Create checkboxes for each action
            for action in ['print', 'eval', 'commit']:
                var_pointer[action] = tk.BooleanVar()
                tk.Checkbutton(checkbox_row, variable=var_pointer[action]).pack(side='left')
                tk.Label(checkbox_row, text=action).pack(side="left", padx=(0,4))

            # Add engine dropdown if required
            if include_engine:
                var_pointer['engine'] = tk.StringVar(value='test')
                tk.OptionMenu(checkbox_row, var_pointer['engine'], 'test', 'prod').pack(padx=(80,0))

            # Add checkbox row to stack
            checkbox_rows_stack.append(checkbox_row)

        #--------------------------------------------#
        button_rows_stack = []
        def create_buttons_row(frame_pointer, actions_matrix, function_subspace):

            # Create now button row in stack
            button_row = tk.Frame(root)

            # Container frame to hold buttons in a grid
            button_row = tk.Frame(frame_pointer)
            button_row.pack(pady=(0,0), anchor='w')

            # Buttons packed horizontally
            for row, col, wid, action in actions_matrix:
                tk.Button(button_row, width=wid, text=action, command=lambda a=action: on_button_click(f'{function_subspace} {a}')).grid(row=row, column=col, padx=(0,0), pady=(0,0), sticky='w')

            # Add button row to stack
            button_rows_stack.append(button_row)

        #----------------------------#
        # 🛎️ 🖥️ Button click handlers #
        #----------------------------#
        def on_button_click(button_input_action):

            # Print action
            print(f"\n🛎️  Button clicked: {button_input_action}")

            #---------------------#
            # Orchestration panel #
            #---------------------#
            if 'orchestration' in button_input_action:

                # Assemble configuration JSON from GUI inputs
                config_json = {'nodes':[], 'edges':[]} 
                for d in gui['orchestration']['node_types']['list']:
                    config_json['nodes'] += [(d['dropdowns'][0].get(), d['process_fields'].get(), d['process_scores'].get())]
                for d in gui['orchestration']['edge_types']['list']:
                    config_json['edges'] += [(d['dropdowns'][0].get(), d['dropdowns'][1].get(), d['process_fields'].get(), d['process_scores'].get())]

                # Orchestration reset
                if button_input_action == 'orchestration reset config':

                    # Print and execute method
                    print("\n🖥️  ~ gr.orchestrator.reset()")
                    gr.orchestrator.reset()

                # Orchestration config
                elif button_input_action == 'orchestration apply config':

                    # Print configuration JSON extracted from GUI
                    print('\nconfig_json =')
                    rich.print_json(data=config_json)

                    # Print and execute method
                    print("\n🖥️  ~ gr.orchestrator.typeflags.config(config_json)")
                    gr.orchestrator.typeflags.config(config_json)

                    # Print and execute method
                    print("\n🖥️  ~ gr.orchestrator.status()")
                    gr.orchestrator.status()

                # Orchestration reset
                elif button_input_action == 'orchestration sync data':

                    # Print and execute method
                    print("\n🖥️  ~ gr.orchestrator.sync()")
                    gr.orchestrator.sync()

                # Orchestration: refresh tp=1 flags
                elif button_input_action == 'orchestration refresh flags':

                    # Print and execute method
                    print("\n🖥️  ~ gr.orchestrator.refresh()")
                    gr.orchestrator.refresh()

            #------------------------#
            # Cache Management panel #
            #------------------------#
            elif 'cachemanage' in button_input_action:

                # Fetch action flags
                method_actions = ()
                for method_action_name in ['print', 'eval', 'commit']:
                    if gui['processing']['cachemanage']['actions'][method_action_name].get():
                        method_actions += (method_action_name,)

                # Cache management apply views
                if button_input_action == 'cachemanage apply views':

                    # Print and execute method
                    print(f"\n🖥️  ~ gr.cachemanager.apply_views(actions={method_actions})")
                    gr.cachemanager.apply_views(actions=method_actions)

                # Cache management apply formulas
                elif button_input_action == 'cachemanage apply formulas':

                    # Use verbose mode if 'print' action is selected
                    verbose = False
                    if 'print' in method_actions:
                        verbose = True

                    # Reject eval action
                    if 'eval' in method_actions:
                        sysmsg.warning("The method 'apply_formulas' does not support an 'eval' action. Nothing to do.")
                        return
                    
                    # Require commit action
                    if 'commit' not in method_actions:
                        sysmsg.warning("The method 'apply_formulas' requires a 'commit' action. Nothing to do.")
                        return

                    # Print and execute method
                    print(f"\n🖥️  ~ gr.cachemanager.apply_formulas(verbose={verbose})")
                    gr.cachemanager.apply_formulas(verbose=verbose)

                # Cache management calculate scores matrix
                elif button_input_action == 'cachemanage calculate scores matrix':
                    
                    # Fetch types to process
                    types_to_process = gr.orchestrator.typeflags.status(types_only=True)

                    # Loop over all edges and calculate scores matrix
                    for from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type in types_to_process['edges']:

                        # Skip if not scores type
                        if flag_type == 'scores':

                            # Print and execute method
                            print(f"\n🖥️  ~ gr.cachemanager.calculate_scores_matrix(from_object_type='{from_object_type}', to_object_type='{to_object_type}, actions={method_actions})")
                            gr.cachemanager.calculate_scores_matrix(from_object_type=from_object_type, to_object_type=to_object_type, actions=method_actions)

                # Cache management consolidate scores matrix
                elif button_input_action == 'cachemanage consolidate scores matrix':
                    types_to_process = gr.orchestrator.typeflags.status(types_only=True)
                    for from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type in types_to_process['edges']:
                        if flag_type == 'scores':
                            print(f"""gr.cachemanager.consolidate_scores_matrix(from_object_type='{from_object_type}', to_object_type='{to_object_type}')""")
                            gr.cachemanager.consolidate_scores_matrix(from_object_type=from_object_type, to_object_type=to_object_type)

            #-----------------------------#
            # GraphIndex Management panel #
            #-----------------------------#
            elif 'indexdb' in button_input_action:

                # Fetch action flags
                method_actions = ()
                for method_action_name in ['print', 'eval', 'commit']:
                    if gui['processing']['indexdb']['actions'][method_action_name].get():
                        method_actions += (method_action_name,)

                # IndexDB cache buildup info
                if button_input_action == 'indexdb cache_buildup info':
                    pass

                # IndexDB cache buildup build all docs and link fields
                elif button_input_action == 'indexdb cache_buildup build all':

                    # Print and execute method
                    print(f"\n🖥️  ~ gr.indexdb.cachebuilder.build_all(actions={method_actions})")
                    gr.indexdb.cachebuilder.build_all(actions=method_actions)

                # IndexDB page profile info
                elif button_input_action == 'indexdb page_profile info':
                    pass

                # IndexDB page profile create table
                elif button_input_action == 'indexdb page_profile create table':
                    pass

                # IndexDB page profile patch
                elif button_input_action == 'indexdb page_profile patch':

                    # Print and execute method
                    print(f"\n🖥️  ~ gr.indexdb.pageprofile.patch(actions={method_actions})")
                    gr.indexdb.pageprofile.patch(actions=method_actions)

                # IndexDB index docs info
                elif button_input_action == 'indexdb index_docs info':
                    pass

                # IndexDB index docs create table
                elif button_input_action == 'indexdb index_docs create table':
                    pass

                # IndexDB index docs patch
                elif button_input_action == 'indexdb index_docs patch':
                    
                    # Print and execute method
                    print(f"\n🖥️  ~ gr.indexdb.docs_patch_all(actions={method_actions})")
                    gr.indexdb.docs_patch_all(actions=method_actions)

                # IndexDB index doc-links info
                elif button_input_action == 'indexdb index_doc_links info':
                    pass

                # IndexDB index doc-links create table
                elif button_input_action == 'indexdb index_doc_links create table':
                    pass
                
                # IndexDB index doc-links horizontal patch
                elif button_input_action == 'indexdb index_doc_links horizontal patch':

                    # Print and execute method
                    print(f"\n🖥️  ~ gr.indexdb.doclinks_horizontal_patch_all(actions={method_actions})")
                    gr.indexdb.doclinks_horizontal_patch_all(actions=method_actions)

                # IndexDB index doc-links vertical patch
                elif button_input_action == 'indexdb index_doc_links vertical patch':

                    # Print and execute method
                    print(f"\n🖥️  ~ gr.indexdb.doclinks_vertical_patch_all(actions={method_actions})")
                    gr.indexdb.doclinks_vertical_patch_all(actions=method_actions)

                # IndexDB create mixed views
                elif button_input_action == 'indexdb create mixed views':
                    pass

                # IndexDB copy patches to prod
                elif button_input_action == 'indexdb copy patches to prod':
                    pass

    #--------------------------------#
    # Subframe (left): Orchestration #
    #--------------------------------#
    if True:

        # Labeled subframe
        gui['orchestration']['subframe'] = tk.LabelFrame(root, text="Orchestration", width=500, padx=10, pady=10)
        gui['orchestration']['subframe'].grid_propagate(False)
        gui['orchestration']['subframe'].grid(row=0, column=0, sticky='ns', padx=(16,8), pady=(16,16))

        # Description inside subframe
        gui['orchestration']['description'] = tk.Label(gui['orchestration']['subframe'],
            text = "Select which type of objects to process, whether to process fields and/or scores, to sync new inserted or deleted data, and to reset or propagate \"to_process\" flags over all cache dependencies.",
            justify='left', anchor='w', fg='gray', wraplength=400
        ).pack(pady=(0,0), anchor='w', fill='x')

        #--------------------------------------------#
        def add_type_to_process(unit, pre_selection=None):

            # Extract pre-selection if provided
            if pre_selection is not None:
                source_node_type, target_node_type, process_fields, process_scores = pre_selection[0], pre_selection[1], 'fields' in pre_selection[2], 'scores' in pre_selection[2]
            else:
                source_node_type, target_node_type, process_fields, process_scores = False, False, False, False

            # Create a new row frame
            row_frame = tk.Frame(gui['orchestration'][f'{unit}_types']['subframe'])
            row_frame.pack(fill='x', pady=0)

            # Dropdown: Source node types
            source_node_type_var = tk.StringVar()
            source_node_dropdown = ttk.Combobox(row_frame, values=list_of_node_types, textvariable=source_node_type_var, width=8)
            source_node_dropdown.grid(row=0, column=0, padx=(0, 0))
            if pre_selection:
                source_node_dropdown.after_idle(lambda: source_node_dropdown.set(source_node_type))

            # Dropdown: Target node types (if edge)
            target_node_dropdown = None
            if unit == 'edge':
                target_node_type_var = tk.StringVar(value=target_node_type if pre_selection else list_of_node_types[0])
                target_node_dropdown = ttk.Combobox(row_frame, values=list_of_node_types, textvariable=target_node_type_var, width=8)
                target_node_dropdown.grid(row=0, column=1, padx=(0, 0))
                if pre_selection:
                    target_node_dropdown.after_idle(lambda: target_node_dropdown.set(target_node_type))

            # Checkbox: Process fields
            pf_var = tk.BooleanVar()
            pf_var.set(bool(process_fields))
            pf_label = tk.Label(row_frame, text="Fields")
            pf_label.grid(row=0, column=2, padx=(0, 0))
            pf_checkbox = tk.Checkbutton(row_frame, variable=pf_var)
            # pf_checkbox.select() if process_fields else pf_checkbox.deselect()
            pf_checkbox.grid(row=0, column=3, padx=(0, 0))

            # Checkbox: Process scores
            ps_var = tk.BooleanVar()
            ps_var.set(bool(process_scores))
            ps_label = tk.Label(row_frame, text="Scores")
            ps_label.grid(row=0, column=4, padx=(0, 0))
            ps_checkbox = tk.Checkbutton(row_frame, variable=ps_var)
            # ps_checkbox.select() if process_scores else ps_checkbox.deselect()
            ps_checkbox.grid(row=0, column=5, padx=(0, 0))

            # Remove button
            def remove_this_row():
                row_frame.destroy()
                gui['orchestration'][f'{unit}_types']['list'].remove(row_data)

            remove_button = tk.Button(row_frame, text="Remove", command=remove_this_row)
            remove_button.grid(row=0, column=6, padx=(0, 0))

            # Optionally store widget references if needed later
            # Store row data
            row_data = {
                'frame': row_frame,
                'dropdowns': [source_node_dropdown, target_node_dropdown],
                'process_fields': pf_var,
                'process_scores': ps_var,
            }
            gui['orchestration'][f'{unit}_types']['list'].append(row_data)

        # Sub-subframe: Node types to process
        if True:

            # Labeled subframe
            gui['orchestration']['node_types']['subframe'] = tk.LabelFrame(gui['orchestration']['subframe'], text="Node types to process", width=480, padx=10, pady=10)
            gui['orchestration']['node_types']['subframe'].pack_propagate(False)
            gui['orchestration']['node_types']['subframe'].pack(padx=10, pady=(10,6), fill='y', expand=True)

            # Description inside subframe
            gui['orchestration']['node_types']['description'] = tk.Label(gui['orchestration']['node_types']['subframe'],
                text = f"Add and remove node types to process.",
                justify='left', anchor='w', fg='gray', wraplength=380
            ).pack(pady=(0,0), anchor='w', fill='x')

            # Buttons to add/remove dropdowns
            gui['orchestration']['node_types']['button_add_new'] = tk.Button(gui['orchestration']['node_types']['subframe'],
                text = "Add node type",
                command = (lambda: add_type_to_process('node'))
            ).pack(pady=(10, 5), anchor='w')

        # Sub-subframe: Edge types to process
        if True:

            # Labeled subframe
            gui['orchestration']['edge_types']['subframe'] = tk.LabelFrame(gui['orchestration']['subframe'], text=f"Edge types to process", width=480, padx=10, pady=10)
            gui['orchestration']['edge_types']['subframe'].pack_propagate(False)
            gui['orchestration']['edge_types']['subframe'].pack(padx=10, pady=(6,12), fill='y', expand=True)

            # Description inside subframe
            gui['orchestration']['edge_types']['description'] = tk.Label(gui['orchestration']['edge_types']['subframe'],
                text = f"Add and remove edge types to process.",
                justify='left', anchor='w', fg='gray', wraplength=380
            ).pack(pady=(0,0), anchor='w', fill='x')

            # Buttons to add/remove dropdowns
            gui['orchestration']['edge_types']['button_add_new'] = tk.Button(gui['orchestration']['edge_types']['subframe'],
                text = "Add edge type",
                command = (lambda: add_type_to_process('edge'))
            ).pack(pady=(10, 5), anchor='w')

        # Create buttons row for cache management actions
        create_buttons_row(
            frame_pointer  = gui['orchestration']['subframe'],
            actions_matrix = [
                (0, 0, 7, 'reset config'), (0, 1, 7, 'apply config'), (0, 2, 6, 'sync data'), (0, 3, 7, 'refresh flags'),
            ],
            function_subspace = 'orchestration'
        )

        # Function for group concat
        def group_concat(tuples):
            grouped = defaultdict(list)
            for t in tuples:
                prefix, last = t[:-1], t[-1]
                grouped[prefix].append(last)
            return [(*k, tuple(v)) for k, v in grouped.items()]

        # Initialise typeflags from current saved settings
        config_json = gr.orchestrator.typeflags.get_config_json()
        flags_to_options = {(False,False):(), (True,False):('fields',), (False,True):('scores',), (True,True):('fields','scores')}
        typeflags_settings = {
            'nodes' : [(e[0],       flags_to_options[(e[1],e[2])]) for e in config_json['nodes']],
            'edges' : [(e[0], e[1], flags_to_options[(e[2],e[3])]) for e in config_json['edges']],
        }
        # print(typeflags_settings)
        # typeflags_settings = {
        #     k: group_concat(v) for k, v in gr.orchestrator.typeflags.status(types_only=True).items()
        # }
        # print(typeflags_settings)

        # Add node and edge types to process based on typeflags settings
        for tfs in typeflags_settings['nodes']:
            add_type_to_process('node', pre_selection=(tfs[0], None  , tfs[1]))
        for tfs in typeflags_settings['edges']:
            add_type_to_process('edge', pre_selection=(tfs[0], tfs[1], tfs[2]))

    #------------------------------#
    # Subframe (right): Processing #
    #------------------------------#
    if True:

        # Labeled subframe
        gui['processing']['subframe'] = tk.LabelFrame(root, text="Processing", width=440, padx=10, pady=10)
        gui['processing']['subframe'].grid_propagate(False)
        gui['processing']['subframe'].grid(row=0, column=1, sticky='ns', padx=(8,16), pady=(16,16))

        #----------------------------#
        # Subframe: Cache Management #
        #----------------------------#
        if True:

            # Create labeled subframe
            gui['processing']['cachemanage']['subframe'] = tk.LabelFrame(gui['processing']['subframe'], text="Cache management", width=400, height=122, padx=10, pady=10)
            gui['processing']['cachemanage']['subframe'].pack_propagate(False)
            gui['processing']['cachemanage']['subframe'].pack(padx=10, pady=10)

            # Create checkboxes row for cache management actions
            create_action_checkboxes_row(
                frame_pointer = gui['processing']['cachemanage']['subframe'],
                var_pointer   = gui['processing']['cachemanage']['actions']
            )

            # Create buttons row for cache management actions
            create_buttons_row(
                frame_pointer  = gui['processing']['cachemanage']['subframe'],
                actions_matrix = [
                    (0, 0, 9, 'apply views'   ), (0, 1, 16, 'calculate scores matrix'  ),
                    (1, 0, 9, 'apply formulas'), (1, 1, 16, 'consolidate scores matrix')
                ],
                function_subspace = 'cachemanage'
            )

        #------------------------------------------------#
        # Subframe: GraphIndex Management (SQL Database) #
        #------------------------------------------------#
        if True:

            # Labeled subframe
            gui['processing']['indexdb']['subframe'] = tk.LabelFrame(gui['processing']['subframe'], text="GraphIndex management (MySQL)", width=400, height=550, padx=10, pady=10)
            gui['processing']['indexdb']['subframe'].pack_propagate(False)
            gui['processing']['indexdb']['subframe'].pack(padx=10, pady=(10,0))

            # Create checkboxes row for cache management actions
            create_action_checkboxes_row(
                frame_pointer  = gui['processing']['indexdb']['subframe'],
                var_pointer    = gui['processing']['indexdb']['actions'],
                include_engine = True
            )

            # Sub-subframes
            if True:

                # Labeled subframe
                gui['processing']['indexdb']['cache_buildup']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Cache build up", width=380, height=60, padx=4, pady=4)
                gui['processing']['indexdb']['cache_buildup']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['cache_buildup']['subframe'].pack(padx=10, pady=(10,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['cache_buildup']['subframe'],
                    actions_matrix = [
                        (0, 0, 2, 'info'), (0, 1, 5, 'build all'),
                    ],
                    function_subspace = 'indexdb cache_buildup'
                )

                # Labeled subframe
                gui['processing']['indexdb']['page_profile']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Page profiles", width=380, height=60, padx=4, pady=4)
                gui['processing']['indexdb']['page_profile']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['page_profile']['subframe'].pack(padx=10, pady=(2,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['page_profile']['subframe'],
                    actions_matrix = [
                        (0, 0, 2, 'info'), (0, 1, 7, 'create table'), (0, 2, 3, 'patch'),
                    ],
                    function_subspace = 'indexdb page_profile'
                )

                # Labeled subframe
                gui['processing']['indexdb']['index_docs']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Index docs", width=380, height=60, padx=4, pady=4)
                gui['processing']['indexdb']['index_docs']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['index_docs']['subframe'].pack(padx=10, pady=(2,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['index_docs']['subframe'],
                    actions_matrix = [
                        (0, 0, 2, 'info'), (0, 1, 7, 'create table'), (0, 2, 3, 'patch'),
                    ],
                    function_subspace = 'indexdb index_docs'
                )

                # Labeled subframe
                gui['processing']['indexdb']['index_doc_links']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Index doc-links", width=380, height=88, padx=4, pady=4)
                gui['processing']['indexdb']['index_doc_links']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['index_doc_links']['subframe'].pack(padx=10, pady=(2,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['index_doc_links']['subframe'],
                    actions_matrix = [
                        (0, 0, 7, 'info'        ), (0, 1, 10, 'vertical patch'  ),
                        (1, 0, 7, 'create table'), (1, 1, 10, 'horizontal patch'),
                    ],
                    function_subspace = 'indexdb index_doc_links'
                )

            # Create buttons row for cache management actions
            create_buttons_row(
                frame_pointer  = gui['processing']['indexdb']['subframe'],
                actions_matrix = [
                    (0, 0, 12, 'create mixed views'), (0, 1, 13, 'copy patches to prod'),
                ],
                function_subspace = 'indexdb'
            )

        #--------------------------------------------------#
        # Subframe: Graph Index Management (ElasticSearch) #
        #--------------------------------------------------#
        if True:

            # Labeled subframe
            gui['processing']['elasticsearch']['subframe'] = tk.LabelFrame(gui['processing']['subframe'], text="GraphIndex management (ElasticSearch)", width=400, height=330, padx=10, pady=10)
            gui['processing']['elasticsearch']['subframe'].pack_propagate(False)
            gui['processing']['elasticsearch']['subframe'].pack(padx=10, pady=10)

    # Launch GUI
    root.mainloop()
