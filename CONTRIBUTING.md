# Contributing to CPacket Generator

## Submitting a Pull Request

Pull requests to the CPacket Generator repository should be focused on a particular issue or goal and not contain unrelated commits.  It would be best to make sure that no one else is working on the same code before beginning work on an issue, but as there is no guarantee the creators of this repository will be continuting to check on or participate in the project, this is not required.

Since the creators of this repository may not be maintaining it, there may be no one to approve pull requests made to this repository.

When submitting a pull request, describe the testing that was done to ensure that the changes you made are working properly.


## Setting up a local copy of the repository

Fork the repository and clone it to put the code into your local workspace.  When finished working on something, submit a pull request to merge with the Development branch of the repository.

In order to test code for the generator, you will likely need your own instance of EC2 with InfluxDB if the code you are writing involves sending data to the database at all.
