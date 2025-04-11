// Package copy provides the copy command.
package copy

import (
	"context"
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/sync"
	"github.com/spf13/cobra"
)

var (
	createEmptySrcDirs = false
	deleteAfterCopy    = false
)

func init() {
	cmd.Root.AddCommand(commandDefinition)
	cmdFlags := commandDefinition.Flags()
	flags.BoolVarP(cmdFlags, &createEmptySrcDirs, "create-empty-src-dirs", "", createEmptySrcDirs, "Create empty source dirs on destination after copy", "")
	flags.BoolVarP(cmdFlags, &deleteAfterCopy, "delete-after-copy", "", deleteAfterCopy, "Delete source files after successful copy, including identical files", "")
}

var commandDefinition = &cobra.Command{
	Use:   "copy source:path dest:path",
	Short: `Copy files from source to dest, skipping identical files.`,
	// Note: "|" will be replaced by backticks below
	Long: strings.ReplaceAll(`Copy the source to the destination.  Does not transfer files that are
identical on source and destination, testing by size and modification
time or MD5SUM.  Doesn't delete files from the destination. If you
want to also delete files from destination, to make it match source,
use the [sync](/commands/rclone_sync/) command instead.

Note that it is always the contents of the directory that is synced,
not the directory itself. So when source:path is a directory, it's the
contents of source:path that are copied, not the directory name and
contents.

To copy single files, use the [copyto](/commands/rclone_copyto/)
command instead.

If dest:path doesn't exist, it is created and the source:path contents
go there.

For example

    rclone copy source:sourcepath dest:destpath

Let's say there are two files in sourcepath

    sourcepath/one.txt
    sourcepath/two.txt

This copies them to

    destpath/one.txt
    destpath/two.txt

Not to

    destpath/sourcepath/one.txt
    destpath/sourcepath/two.txt

If you are familiar with |rsync|, rclone always works as if you had
written a trailing |/| - meaning "copy the contents of this directory".
This applies to all commands and whether you are talking about the
source or destination.

See the [--no-traverse](/docs/#no-traverse) option for controlling
whether rclone lists the destination directory or not.  Supplying this
option when copying a small number of files into a large destination
can speed transfers up greatly.

For example, if you have many files in /path/to/src but only a few of
them change every day, you can copy all the files which have changed
recently very efficiently like this:

    rclone copy --max-age 24h --no-traverse /path/to/src remote:

Use the |--delete-after-copy| flag to delete source files after successful copy.
Each file will be deleted immediately after it is successfully copied.

Rclone will sync the modification times of files and directories if
the backend supports it. If metadata syncing is required then use the
|--metadata| flag.

Note that the modification time and metadata for the root directory
will **not** be synced. See https://github.com/rclone/rclone/issues/7652
for more info.

**Note**: Use the |-P|/|--progress| flag to view real-time transfer statistics.

**Note**: Use the |--dry-run| or the |--interactive|/|-i| flag to test without copying anything.
`, "|", "`"),
	Annotations: map[string]string{
		"groups": "Copy,Filter,Listing,Important",
	},
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(2, 2, command, args)
		fsrc, srcFileName, fdst := cmd.NewFsSrcFileDst(args)
		// mod
		if len(fsrc.Root()) > 7 && fsrc.Root()[0:7] == "isFile:" {
			srcFileName = fsrc.Root()[7:]
		}
		cmd.Run(true, true, command, func() error {
			if srcFileName == "" {
				// 定义回调函数，在文件复制完成后删除源文件
				callback := func(obj fs.Object) error {
					if deleteAfterCopy {
						return operations.DeleteFile(context.Background(), obj)
					}
					return nil
				}
				// 修改CopyDir调用，添加noCheckDest参数
				return sync.CopyDir(context.Background(), fdst, fsrc, createEmptySrcDirs, callback)
			}
			// 获取源文件对象
			obj, err := fsrc.NewObject(context.Background(), srcFileName)
			if err != nil {
				return err
			}
			// 检查目标文件是否存在
			dstObj, _ := fdst.NewObject(context.Background(), srcFileName)
			if dstObj != nil {
				// 如果目标文件存在且deleteAfterCopy为true，直接删除源文件
				if deleteAfterCopy {
					return operations.DeleteFile(context.Background(), obj)
				}
				return nil
			}
			err = operations.CopyFile(context.Background(), fdst, fsrc, srcFileName, srcFileName)
			if err == nil && deleteAfterCopy {
				return operations.DeleteFile(context.Background(), obj)
			}
			return err
		})
	},
}
