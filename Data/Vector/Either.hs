{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Everything in here has been committed upstream and is
-- already in the master branch of vector. It is still
-- pending release to hackage.

module Data.Vector.Either
  ( partitionWith
  ) where

import Prelude hiding ( length, null, replicate, reverse, map, read,
                        take, drop, splitAt, init, tail )

import           Data.Vector.Fusion.Bundle      (Bundle)
import           Data.Vector.Generic.Mutable.Base
import           Data.Vector.Fusion.Bundle.Size

import           Data.Vector.Generic.Base

import qualified Data.Vector.Generic.Mutable as M
import qualified Data.Vector.Generic as V

import qualified Data.Vector.Fusion.Bundle as Bundle
import Control.Monad.ST (runST)
import Control.Monad.Primitive

partitionWith :: (Vector v a, Vector v b, Vector v c) => (a -> Either b c) -> v a -> (v b, v c)
{-# INLINE partitionWith #-}
partitionWith f = partition_with_stream f . V.stream

partition_with_stream :: (Vector v a, Vector v b, Vector v c) => (a -> Either b c) -> Bundle u a -> (v b, v c)
{-# INLINE [1] partition_with_stream #-}
partition_with_stream f s = s `seq` runST (
  do
    (mv1,mv2) <- partitionWithBundle f s
    v1 <- V.unsafeFreeze mv1
    v2 <- V.unsafeFreeze mv2
    return (v1,v2))

partitionWithBundle :: (PrimMonad m, MVector v a, MVector v b, MVector v c)
        => (a -> Either b c) -> Bundle u a -> m (v (PrimState m) b, v (PrimState m) c)
{-# INLINE partitionWithBundle #-}
partitionWithBundle f s
  = case upperBound (Bundle.size s) of
      Just n  -> partitionWithMax f s n
      Nothing -> partitionWithUnknown f s

partitionWithMax :: (PrimMonad m, MVector v a, MVector v b, MVector v c)
  => (a -> Either b c) -> Bundle u a -> Int -> m (v (PrimState m) b, v (PrimState m) c)
{-# INLINE partitionWithMax #-}
partitionWithMax f s n
  = do
      v1 <- M.unsafeNew n
      v2 <- M.unsafeNew n
      let {-# INLINE [0] put #-}
          put (i1, i2) x = case f x of
            Left b -> do
              M.unsafeWrite v1 i1 b
              return (i1+1, i2)
            Right c -> do
              M.unsafeWrite v2 i2 c
              return (i1, i2+1)
      (n1, n2) <- Bundle.foldM' put (0, 0) s
      return (M.unsafeSlice 0 n1 v1, M.unsafeSlice 0 n2 v2)

partitionWithUnknown :: forall m v u a b c.
     (PrimMonad m, MVector v a, MVector v b, MVector v c)
  => (a -> Either b c) -> Bundle u a -> m (v (PrimState m) b, v (PrimState m) c)
{-# INLINE partitionWithUnknown #-}
partitionWithUnknown f s
  = do
      v1 <- M.unsafeNew 0
      v2 <- M.unsafeNew 0
      (v1', n1, v2', n2) <- Bundle.foldM' put (v1, 0, v2, 0) s
      return (M.unsafeSlice 0 n1 v1', M.unsafeSlice 0 n2 v2')
  where
    put :: (v (PrimState m) b, Int, v (PrimState m) c, Int)
        -> a
        -> m (v (PrimState m) b, Int, v (PrimState m) c, Int)
    {-# INLINE [0] put #-}
    put (v1, i1, v2, i2) x = case f x of
      Left b -> do
        v1' <- unsafeAppend1 v1 i1 b
        return (v1', i1+1, v2, i2)
      Right c -> do
        v2' <- unsafeAppend1 v2 i2 c
        return (v1, i1, v2', i2+1)

unsafeAppend1 :: (PrimMonad m, MVector v a)
        => v (PrimState m) a -> Int -> a -> m (v (PrimState m) a)
{-# INLINE [0] unsafeAppend1 #-}
    -- NOTE: The case distinction has to be on the outside because
    -- GHC creates a join point for the unsafeWrite even when everything
    -- is inlined. This is bad because with the join point, v isn't getting
    -- unboxed.
unsafeAppend1 v i x
  | i < M.length v = do
                     M.unsafeWrite v i x
                     return v
  | otherwise    = do
                     v' <- enlarge v
                     M.unsafeWrite v' i x
                     return v'

-- | Grow a vector logarithmically
enlarge :: (PrimMonad m, MVector v a)
                => v (PrimState m) a -> m (v (PrimState m) a)
{-# INLINE enlarge #-}
enlarge v = do vnew <- M.unsafeGrow v by
               M.basicInitialize $ M.basicUnsafeSlice (M.length v) by vnew
               return vnew
  where
    by = enlarge_delta v


enlarge_delta :: MVector v a => v s a -> Int
enlarge_delta v = max (M.length v) 1
