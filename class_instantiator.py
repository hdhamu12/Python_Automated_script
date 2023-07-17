import importlib


# function to instantiate the object of the class
def instantiate_class(p_class_module: str, p_class_path: str, p_class_params):
    if "." in p_class_path:
        module_name, class_name = p_class_path.rsplit(".", 1)
        Class = getattr(
            importlib.import_module(p_class_module + "." + module_name), class_name
        )

    else:
        Class = getattr(importlib.import_module(p_class_module), p_class_path)
    class_instance = Class(p_class_params)

    return class_instance
