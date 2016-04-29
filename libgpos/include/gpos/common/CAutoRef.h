//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename: 
//		CAutoRef.h
//
//	@doc:
//		Basic auto pointer for ref-counted objects
//
//	@owner: 
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoRef_H
#define GPOS_CAutoRef_H

#include <type_traits>

#include "gpos/base.h"
#include "gpos/common/CAutoPointerBase.h"
#include "gpos/common/CRefCount.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoRef
	//
	//	@doc:
	//		Wrapps pointer of type T which is a subtype of CRefCount
	//
	//---------------------------------------------------------------------------
	template <class T>
	class CAutoRef : public CAutoPointerBase<T>
	{

		private:
			static_assert(std::is_base_of<CRefCount, T>::value, "T must be a CRefCount");

			typedef CAutoPointerBase<T> _base;

			// hidden copy ctor
			CAutoRef<T>
				(
				const CAutoRef&
				);

		public:
		
			// ctor
			explicit
			CAutoRef<T>()
				:
				_base()
			{}

			// ctor
			explicit
			CAutoRef<T>(T *pt)
				:
				_base(pt)
			{}

			virtual ~CAutoRef();

			// simple assignment
			CAutoRef<T> const & operator = (T* pt)
			{
				_base::m_pt = pt;
				return *this;
			}

	}; // class CAutoRef

	//---------------------------------------------------------------------------
	//	@function:
	//		CAutoRef::~CAutoRef
	//
	//	@doc:
	//		Dtor
	//
	//---------------------------------------------------------------------------
	template <class T>
	CAutoRef<T>::~CAutoRef()
	{
		if (NULL != _base::m_pt)
		{
			_base::m_pt->Release();
		}
	}
}

#endif // !GPOS_CAutoRef_H

// EOF

