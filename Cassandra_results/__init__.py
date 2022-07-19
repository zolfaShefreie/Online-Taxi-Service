import os

package_dir = os.path.dirname(
    os.path.abspath(__file__)
)
static = os.path.join(
    package_dir, "static"
)
app.static_folder=static