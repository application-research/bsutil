package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cheggaaa/pb/v3"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()
	app.Commands = []*cli.Command{

		{
			Name:  "peek",
			Usage: "Peek at the contents of a blockstore",
			Flags: []cli.Flag{
				&cli.StringSliceFlag{
					Name:     "input",
					Usage:    "Path to the input blockstore",
					Required: true,
					Aliases:  []string{"i"},
				},
			},
			Action: cmdPeek,
		},
		{
			Name:        "merge",
			Description: "Merge two or more flatfs blockstores into one",
			Usage:       "bsutil merge -i /blockstore-a -i /blockstore-b -i /blockstore-c -o /optional-output-path",
			Flags: []cli.Flag{
				&cli.StringSliceFlag{
					Name:    "input",
					Aliases: []string{"i"},
				},
				&cli.StringFlag{
					Name:    "output",
					Aliases: []string{"o"},
					Value:   "./merged-blockstore",
				},
			},
			Action: cmdMerge,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("%v\n", err)
	}
}

func cmdPeek(ctx *cli.Context) error {
	if len(ctx.StringSlice("input")) == 0 {
		return fmt.Errorf("at least one input is required")
	}

	for i, inputPath := range ctx.StringSlice("input") {
		inputDS, err := flatfs.Open(inputPath, false)
		if err != nil {
			return err
		}

		input := blockstore.NewBlockstoreNoPrefix(inputDS)

		fmt.Printf("Peeking at %s... (%d/%d)\n", inputPath, i+1, len(ctx.StringSlice("input")))
		allLMDBKeys, err := input.AllKeysChan(ctx.Context)
		if err != nil {
			return fmt.Errorf("could not get all lmdb keys channel: %v", err)
		}
		for key := range allLMDBKeys {
			fmt.Println(key) // just want to look at it.
		}

		// Close the input datastore - no sync required since it's only being read from
		if err := inputDS.Close(); err != nil {
			fmt.Printf("Failed to close input blockstore %d\n", i)
		}
	}

	return nil
}

func cmdMerge(ctx *cli.Context) error {

	// Create the output blockstore and make sure it doesn't already exist
	if err := flatfs.Create(ctx.String("output"), flatfs.NextToLast(3)); err != nil {
		return fmt.Errorf("could not open output blockstore: %v", err)
	}

	// Open the output blockstore
	outputDS, err := flatfs.Open(ctx.String("output"), false)
	if err != nil {
		return err
	}

	output := blockstore.NewBlockstoreNoPrefix(outputDS)

	if len(ctx.StringSlice("input")) == 0 {
		return fmt.Errorf("at least one input is required")
	}

	// Open the input blockstores
	for i, inputPath := range ctx.StringSlice("input") {
		inputDS, err := flatfs.Open(inputPath, false)
		if err != nil {
			return err
		}

		inputSize, err := dirSize(inputPath)
		if err != nil {
			return err
		}

		input := blockstore.NewBlockstoreNoPrefix(inputDS)

		fmt.Printf("Merging %s... (%d/%d)\n", inputPath, i+1, len(ctx.StringSlice("input")))
		if err := transferBlocks(ctx.Context, input, output, inputSize); err != nil {
			return err
		}

		// Close the input datastore - no sync required since it's only being read from
		if err := inputDS.Close(); err != nil {
			fmt.Printf("Failed to close input blockstore %d\n", i)
		}
	}

	// Make sure the output is synced before closing
	if err := outputDS.Sync(ctx.Context, datastore.NewKey("/")); err != nil {
		return err
	}

	if err := outputDS.Close(); err != nil {
		return err
	}

	fmt.Printf("Finished merging\n")

	return nil
}

func transferBlocks(
	ctx context.Context,
	from blockstore.Blockstore,
	to blockstore.Blockstore,
	size int64,
) error {

	allLMDBKeys, err := from.AllKeysChan(ctx)
	if err != nil {
		return fmt.Errorf("could not get all lmdb keys channel: %v", err)
	}

	var buffer []blocks.Block

	bar := pb.New64(size).Set(pb.Bytes, true).Set(pb.CleanOnFinish, true)
	bar.Start()
	defer bar.Finish()

	for cid := range allLMDBKeys {
		block, err := from.Get(ctx, cid)
		if err != nil {
			return fmt.Errorf("could not get expected block '%s' from lmdb blockstore: %v", cid, err)
		}

		buffer = append(buffer, block)
		if len(buffer) >= 100 {
			if err := to.PutMany(ctx, buffer); err != nil {
				return fmt.Errorf("could not write block '%s' to flatfs blockstore: %v", cid, err)
			}
			buffer = nil
		}

		bar.Add(len(block.RawData()))
	}

	return nil
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
